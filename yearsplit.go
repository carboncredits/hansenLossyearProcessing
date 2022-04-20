package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sync"

	"github.com/lukeroth/gdal"
	"github.com/schollz/progressbar/v3"
)

func datasetToTiles(outputFilename string, dataset gdal.Dataset) error {

	tiles_dataset, err := gdal.Translate(outputFilename, dataset, nil)
	if err != nil {
		return fmt.Errorf("failed to translate dataset for %s: %w", outputFilename, err)
	}

	// now we want to generate more levels. The following is based on the
	// source code to gdaladdo:
	// https://github.com/OSGeo/gdal/blob/master/apps/gdaladdo.cpp

	overviewFactor := 1.0
	overviewLevels := make([]int, 0)
	minTileSize := 256

	bands := tiles_dataset.RasterCount()
	bandList := make([]int, bands)
	for index := 0; index < bands; index += 1 {
		bandList[index] = index
	}

	width := tiles_dataset.RasterXSize()
	height := tiles_dataset.RasterYSize()

	for (int(math.Ceil(float64(width)/overviewFactor)) > minTileSize) &&
		(int(math.Ceil(float64(height)/overviewFactor)) > minTileSize) {
		overviewFactor *= 2
		overviewLevels = append(overviewLevels, int(overviewFactor))
	}

	if bands > 0 {
		gdal.CPLSetConfigOption("USE_RRD", "YES")
	}

	if len(overviewLevels) > 0 {
		err := tiles_dataset.BuildOverviews(
			"nearest",
			len(overviewLevels),
			overviewLevels,
			bands,
			bandList,
			gdal.DummyProgress,
			nil)
		if err != nil {
			return fmt.Errorf("failed to build overviews for %s: %w", outputFilename, err)
		}
	}
	tiles_dataset.Close()

	return nil
}

func main() {

	var cores int
	flag.IntVar(&cores, "j", 4, "Parallel cores to use")

	flag.Parse()
	filename := flag.Arg(0)
	if filename == "" {
		fmt.Printf("Usage: %s [-j cores] [filename]\n", os.Args[0])
		return
	}

	outputDriver, err := gdal.GetDriverByName("GTiff")
	if err != nil {
		log.Fatalf("Failed to load GeoTIFF driver: %v", err)
	}

	yearData, err := gdal.Open(filename, gdal.ReadOnly)
	if err != nil {
		log.Fatalf("Failed to open %s: %v", filename, err)
	}
	defer yearData.Close()

	bands := yearData.RasterCount()
	fmt.Printf("Bands: %d\n", bands)
	width := yearData.RasterXSize()
	height := yearData.RasterYSize()
	fmt.Printf("Processing %d x %d area\n", width, height)

	raster := yearData.RasterBand(1)
	width = raster.XSize()
	height = raster.YSize()
	fmt.Printf("Processing %d x %d area\n", width, height)
	blockWidth, blockHeight := raster.BlockSize()
	fmt.Printf("Block size %d x %d\n", blockWidth, blockHeight)

	// To start simply, we'll use RasterBand.IO as a Go typed interface to
	// get data. At some point this might be better to switch to RasterBand.ReadBlock
	// for perfomance reasons, but that'll require using unsafe pointers, so
	// we leave that for a future optimisation.
	//
	// In the Hansen data I'm looking at the block size happens to be a multiple of line width
	// so I'm using that as my chunk size to make it easier to migrate to ReadBlock
	// later if I need the performance.

	minYear := uint8(1)
	maxYear := uint8(20)

	// we want to name the files similar to what Hansen does, so
	// we'll use this data to derive the filename
	transform := yearData.GeoTransform()
	long := fmt.Sprintf("%d", (int(transform[0])+360)%360)
	lat := fmt.Sprintf("%d", (int(transform[3])+360)%360)

	datasets := make([]gdal.Dataset, (maxYear-minYear)+1)
	for year := minYear; year <= maxYear; year += 1 {
		filename := fmt.Sprintf("accumulative_lossyear_to_2%03d_%s_%s.tiff", year, lat, long)
		if _, err := os.Stat(filename); err == nil {
			log.Fatalf("Dataset %s already exists, aborting.", filename)
		}
		dataset := outputDriver.Create(filename, width, height, 1, gdal.Byte, nil)
		dataset.SetProjection(yearData.Projection())
		dataset.SetGeoTransform(transform)
		datasets[year-minYear] = dataset
	}

	yearBuffers := make([][]uint8, (maxYear-minYear)+1)
	for year := minYear; year <= maxYear; year += 1 {
		yearBuffers[year-minYear] = make([]uint8, width)
	}

	blockBuffer := make([]uint8, width*1)
	bar := progressbar.Default(int64(height))
	for line := 0; line < height; line += 1 {
		err := raster.IO(gdal.Read, 0, line, width, 1, blockBuffer, width, 1, 0, 0)
		if err != nil {
			log.Fatalf("Failed to read data: %v", err)
		}

		wg := new(sync.WaitGroup)
		for core := 0; core < cores; core++ {
			wg.Add(1)
			bucketSize := width / cores
			offset := core * bucketSize
			// account for aliasing errors
			slop := width - (bucketSize * (core + 1))
			if slop < bucketSize {
				bucketSize += slop
			}
			go func() {
				for index := 0; index < bucketSize; index += 1 {
					val := blockBuffer[offset+index]
					for year := minYear; year <= maxYear; year += 1 {
						yearVal := 0
						if (val >= minYear) && (val <= year) {
							yearVal = 255
						}
						yearBuffers[year-minYear][offset+index] = uint8(yearVal)
					}
				}

				wg.Done()
			}()
		}
		wg.Wait()

		for year := minYear; year <= maxYear; year += 1 {
			dataset := datasets[year-minYear]
			raster := dataset.RasterBand(1)
			err := raster.IO(gdal.Write, 0, line, width, 1, yearBuffers[year-minYear], width, 1, 0, 0)
			if err != nil {
				log.Fatalf("Failed to write buffer for year 2%03d: %v", year, err)
			}
		}
		bar.Add(1)
	}

	tilesbar := progressbar.Default(int64(maxYear-minYear) + 1)
	sem := make(chan struct{}, cores)
	wg := new(sync.WaitGroup)
	for year := minYear; year <= maxYear; year += 1 {
		dataset := datasets[year-minYear]
		filename := fmt.Sprintf("accumulative_lossyear_to_2%03d_%s_%s.%s", year, lat, long, "mbtiles")
		wg.Add(1)
		go func() {
			sem <- struct{}{}

			err := datasetToTiles(filename, dataset)
			dataset.Close()
			if err != nil {
				log.Fatalf("Failed to generate tiles: %v", err)
			}
			<-sem

			tilesbar.Add(1)
			wg.Done()
		}()
	}
	wg.Wait()

}
