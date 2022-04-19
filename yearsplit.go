package main

import (
	"C"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/lukeroth/gdal"
)

func main() {

	flag.Parse()
	filename := flag.Arg(0)
	if filename == "" {
		fmt.Printf("Usage: %s [filename]\n", os.Args[0])
		return
	}

	geotiffDriver, err := gdal.GetDriverByName("GTiff")
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
		dataset := geotiffDriver.Create(filename, width, height, 1, gdal.Byte, nil)
		dataset.SetProjection(yearData.Projection())
		dataset.SetGeoTransform(transform)
		datasets[year-minYear] = dataset
	}

	yearBuffers := make([][]uint8, (maxYear-minYear)+1)
	for year := minYear; year <= maxYear; year += 1 {
		yearBuffers[year-minYear] = make([]uint8, width)
	}

	blockBuffer := make([]uint8, width*1)
	for line := 0; line < height; line += 1 {
		err := raster.IO(gdal.Read, 0, line, width, 1, blockBuffer, width, 1, 0, 0)
		if err != nil {
			log.Fatalf("Failed to read data: %v", err)
		}
		for index := 0; index < width; index++ {
			val := blockBuffer[index]
			for year := minYear; year <= maxYear; year += 1 {
				yearVal := 0
				if (val >= minYear) && (val <= year) {
					yearVal = 255
				}
				yearBuffers[year-minYear][index] = uint8(yearVal)
			}
		}

		for year := minYear; year <= maxYear; year += 1 {
			dataset := datasets[year-minYear]
			raster := dataset.RasterBand(1)
			err := raster.IO(gdal.Write, 0, line, width, 1, yearBuffers[year-minYear], width, 1, 0, 0)
			if err != nil {
				log.Fatalf("Failed to write buffer for year 2%03d: %v", year, err)
			}
		}
		fmt.Printf("%g%%\n", float64(line)/float64(width)*100)
	}

	for year := minYear; year <= maxYear; year += 1 {
		datasets[year-minYear].Close()
	}
}
