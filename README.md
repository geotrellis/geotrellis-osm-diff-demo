# OSM Diff Demo

Diffs OSM data against a GeoJSON file and generates a vector tile layer of the results.

## Setup

Ensure you have an AWS profile configured with at least full S3 access. If you're running
in EMR (see below) you'll also need to ensure full permissions for EMR.

## Running

You'll need a line delimited GeoJSON file. This can be generated from your source GeoJSON with
[`tippecanoe-json-tool`](https://github.com/mapbox/tippecanoe#tippecanoe-json-tool)
or a number of other tools.

You'll also need an OSM extract that covers the area of your GeoJSON extent. Country extracts
can be pulled from https://www.geofabrik.de/data/download.html. For smaller areas, try:
- https://www.interline.io/osm/extracts/
- https://www.nextzen.org/metro-extracts/index.html

Once you've got your extract, run it through [osm2orc](https://github.com/mojodna/osm2orc) to generate an ORC file.

At this point you have two options. If you have a decent desktop with 16Gb of RAM or more and are only running 
GeoJSON and ORC files up to ~100mb, you can probably run locally. Much larger areas will require a spark cluster.

## Local

Start SBT shell with `./sbt`, then run:
```
test:runMain oiosmdiff.Main <path to orc file> <path to geojson file> <s3OutputPathPrefix>
```

Note that the paths to the OSM and GeoJSON file should be prefixed with `file://` and point to local files. 

## EMR

__UNTESTED__

Start SBT shell with `./sbt`, then run:
```
sparkSubmit <s3 path to orc file> <s3 path to geojson file> <s3OutputPathPrefix>
```

Note that when running in this mode, every argument above should start with `s3://`.
 
