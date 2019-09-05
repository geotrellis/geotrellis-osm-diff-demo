# VectorPipe Export Benchmarks for California

This document notes demo timings for the [VectorPipe pipeline API](https://github.com/geotrellis/vectorpipe/blob/v1.0.0-RC3/src/main/scala/vectorpipe/vectortile/Pipeline.scala)
against Tippecanoe export for the [OSM Diff demo](https://github.com/geotrellis/geotrellis-osm-diff-demo).

The goal of this demo is to show GT/VectorPipe APIs that can be used for:

- Generating vector tile layers from a combination of varying input datasets
- Querying and retrieving OSM data via the Spark DataFrame API
- Performing aggregations on the resulting geometries at each zoom level when
  exporting vector tiles (via a weighted centroid algorithm in this case)

For both cases, we generated tiles for zoom levels 1 - 12, which resulted in ~8000 tiles for CA. For the VectorPipe implementation, zoom level 12 contains the building polygons and all other zoom levels contain the weighted centroids.

The VectorPipe weighted centroid implementation for dropping and consolidating geometries as we zoom out
is very similar in spirit to the one used by Tippecanoe's `--drop-densest-as-needed` argument. Both strategies
aggregate by binning via a z-curve and then dropping all but one to a few geometries for each index.

To compare, I ran the following two commands as Spark jobs:

```bash
# VectorPipe direct output to S3
sparkSubmit --buildings https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/California.zip \
  s3://geotrellis-test/oi-osm-diff/data/osm/california-latest.orc \
  s3://geotrellis-test/oi-osm-diff/vectortiles/spark-msft-buildings-california-v03

# GeoJson output to S3
sparkSubmit --buildings https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/California.zip \
  --outputFormat GeoJson \
  s3://geotrellis-test/oi-osm-diff/data/osm/california-latest.orc \
  s3://geotrellis-test/oi-osm-diff/geojson/spark-msft-buildings-california-v01
```

On a cluster with 20 m4.2xlarge workers the VectorPipe job completed in 4.5 hours. The GeoJson job completed in 20 minutes.

However, the story isn't finished for the GeoJson run. We need to concat the output geojson files, generate a tileset and upload to S3 ourselves.

To do so, we run the following:

```bash
# cd to a dir of your choosing, then:

# Download geojson files
aws s3 cp s3://geotrellis-test/oi-osm-diff/geojson/spark-msft-buildings-california-v01 ./ --recursive

# Generate mbtiles file with tippecanoe
time cat ./part-*.txt | tippecanoe -o california-diff.mbtiles -z14 --drop-densest-as-needed --coalesce --reorder --hilbert --force -s EPSG:3857 -P
# (11034969 features, 478106544 bytes of geometry, 4 bytes of separate metadata, 33 bytes of string pool)
# ~20 mins to run

# Create xyz pbf tileset
# < 1 min to run
mb-util --image_format=pbf california-diff.mbtiles california-diff.tiles

# Upload to S3
# Don't forget the public ACL and content-encoding metadata flags!
# ~5 mins to run
aws s3 cp \
  ./california-diff.tiles \
  s3://geotrellis-test/oi-osm-diff/vectortiles/tippecanoe-msft-buildings-california-v01/ \
  --recursive \
  --acl public-read \
  --content-encoding gzip
```

In summary, the VectorPipe RC3 implementation runs in 4.5hrs on a spark cluster vs ~20 mins for Spark + ~30 mins to generate and upload tiles via Tippecanoe + mbutil + S3 CLI. A visual comparison can be viewed here: https://s3.amazonaws.com/geotrellis-test/oi-osm-diff/demos/california/index.html

After further benchmarking outside the scope of this project, it was determined that it's likely that the primary source of slowness in VectorPipe is the custom WeightedCentroid code used to aggregate polygons before export. Other strategies or optimizations may significantly improve the VectorPipe version.

## Entire US Benchmarks

I was able to run the GeoJson + Tippecanoe version of this workflow for the entire US. The Spark cluster generated 41Gb of GeoJson in S3 in approximately 30 minutes. Tippecanoe, using the same parameters as above plus -t ./tmp (to put the tmp directory on the large EC2 instance volume), took ~80 min to generate a 2Gb MBTiles file. Again, mbutil ran in < 1 min, and the S3 push of 177k tiles in the pbf xyz pyramid took approximately 10mins.

Given that the time to beat is ~80 min (tippecanoe) + 1 min mbutil + ~10 min (s3 push) for a total of 90 mins aggregating, tiling and saving the entire US to S3. We disregard the ~30 min to generate the GeoJson files from the source OSM and MSFT buildings data since we do that in both pipelines.
