Summary
----

A tool to take the Hansen forest loss data[0], which is encoded thus:

> Forest loss during the period 2000–2020, defined as a stand-replacement
> disturbance, or a change from a forest to non-forest state. Encoded as
> either 0 (no loss) or else a value in the range 1–20, representing loss
> detected primarily in the year 2001–2020, respectively.

And to turn into a series of cummulative datasets, which represents the loss from the start of monitoring (2000) until each year, to facilitate the visualisation of loss over time.

Dependancies:
----

Requires a Go compiler and libgdal-dev.

Usage
-----

For this to work, download a lossyear square from the Hansen data[0], and then run it like thus:

```
./yearloss [tilename]
```

References
----

[0] https://data.globalforestwatch.org/documents/tree-cover-loss/explore
