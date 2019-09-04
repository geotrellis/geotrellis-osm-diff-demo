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
test:runMain osmdiff.Main --buildings <URL to MSFT buildings zip file> <s3 path to orc file> <s3OutputPathPrefix>
```

Note that the paths to the OSM and GeoJSON file should be prefixed with `file://` and point to local files.

## EMR

Start SBT shell with `./sbt`, then run:

```
sparkSubmit --buildings <URL to MSFT buildings zip file> <s3 path to orc file> <s3OutputPathPrefix>
```

Note that when running in this mode, each S3 argument above should start with `s3://`. If you're generating GeoJson output, see below for generating vector tiles from the GeoJson.

### Generating tiles from GeoJson output

You'll need to set Spark to export GeoJson with `--outputFormat GeoJson` if you want to run the entire US as the VectorPipe version currently doesn't complete. In order to generate vector tiles from the GeoJson, you'll need to run them through tippecanoe + mbutils + manually upload to S3.

The best way to do this is to spin up an EC2 on demand instance (we used `m5d.2xlarge`) with >250gb scratch space to hold all the files. The tooling you need can then be installed with [./scripts/install_ec2.sh](./scripts/install_ec2.sh).

With the dependencies installed, you can run the same commands as demonstrated in [./BENCHMARKING.md](./BENCHMARKING.md) after pulling the GeoJson from S3 via the AWS CLI. Note that you'll need to set the tippecanoe tmp working directory to the mounted scratch disk with `-t <tmpdir location>` to avoid running out of disk space.
