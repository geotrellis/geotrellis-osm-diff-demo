package geotrellis.batch

import geotrellis.raster._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark._
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme, LayoutDefinition}
import geotrellis.proj4.WebMercator
import geotrellis.vector.ProjectedExtent

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ProcessInputs {
  def apply(
      paths: Seq[String],
      targetZoom: Int,
      numPartitions: Option[Int]
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    val floatingLayout = FloatingLayoutScheme(256)
    val scheme         = ZoomedLayoutScheme(WebMercator)

    val targetLayout: LayoutDefinition = scheme.levelForZoom(targetZoom).layout

    val baseRDD: RDD[(ProjectedExtent, MultibandTile)] =
      sc.parallelize(paths, numPartitions.getOrElse(paths.size))
        .mapPartitions({ partitions =>
          partitions.map { path =>
            val geoTiff         = MultibandGeoTiff(path, streaming = true)
            val projectedExtent = ProjectedExtent(geoTiff.extent, geoTiff.crs)

            (projectedExtent, geoTiff.tile)
          }
        })

    val metadata: TileLayerMetadata[SpatialKey] =
      baseRDD.collectMetadata[SpatialKey](floatingLayout)._2
    val tiledRDD: MultibandTileLayerRDD[SpatialKey] = baseRDD.tileToLayout(metadata)

    tiledRDD.reproject(WebMercator, targetLayout)._2
  }
}
