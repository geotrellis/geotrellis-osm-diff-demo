/**
  * This file implements a VectorPipe Pipeline to generate a vector tile layer of MSFT
  * building geometries tagged with whether there is a matching building in OSM.
  */
package osmdiff

import java.net.URI

import geotrellis.raster.RasterExtent
import geotrellis.spark.tiling.LayoutLevel
import geotrellis.vector.{Feature, Geometry}
import geotrellis.vectortile.{VBool, VInt64}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{array, col, explode, lit, sum, udf}
import org.locationtech.jts.{geom => jts}
import vectorpipe.vectortile.{Pipeline, SingleLayer, VectorTileFeature, getSpatialKey}

private case class Bin(x: Int, y: Int)
private object Bin {
  def apply(tup: (Int, Int)): Bin = Bin(tup._1, tup._2)
}

/**
  * For zoom >= 12, passthrough Building polygons and attach the hasOsm property from the input DataFrame
  * For zoom >= 0, compute aggregate weighted centroid of all geoms in each SpatialKey tile
  */
case class BuildingsDiffPipeline(geometryColumn: String, baseOutputURI: URI, gridResolution: Int)
    extends Pipeline {

  val layerMultiplicity = SingleLayer("msft_buildings")
  val maxZoom = 12

  val weightedCentroid = new WeightedCentroid

  override def pack(row: Row, zoom: Int): VectorTileFeature[Geometry] = {
    val jtsGeom: jts.Geometry = row.getAs[jts.Geometry](geometryColumn)

    if (zoom >= maxZoom) {
      val hasOsm = row.getAs[Boolean]("has_osm")
      Feature(Geometry(jtsGeom), Map("hasOsm" -> VBool(hasOsm)))
    } else {
      val hasOsm = row.getAs[Boolean]("has_osm")
      val weight = row.getAs[Long]("weight")
      Feature(Geometry(jtsGeom), Map("hasOsm" -> VBool(hasOsm), "weight" -> VInt64(weight)))
    }
  }

  override def reduce(input: DataFrame, layoutLevel: LayoutLevel, keyColumn: String): DataFrame = {
    import input.sparkSession.implicits._

    if (layoutLevel.zoom >= maxZoom) {
      input
    } else {
      val layout = layoutLevel.layout
      val extentResolution: Int = gridResolution / 4
      val binOfTile = udf { (g: jts.Geometry, key: GenericRowWithSchema) =>
        val p = g.getCentroid
        val k  = getSpatialKey(key)
        val re = RasterExtent(layout.mapTransform.keyToExtent(k), extentResolution, extentResolution)
        val c  = p.getCentroid.getCoordinate
        Bin(re.mapToGrid(c.x, c.y))
      }

      val st_centroid = udf { g: jts.Geometry => g.getCentroid }

      input
        .where("has_osm = true")
        .withColumn(keyColumn, explode(col(keyColumn)))
        .withColumn("bin", binOfTile(col(geometryColumn), col(keyColumn)))
        .groupBy(col(keyColumn), col("bin"))
        .agg(sum('weight) as 'weight,
             weightedCentroid(st_centroid(col(geometryColumn)), 'weight) as geometryColumn)
        .drop('bin)
        .withColumn(keyColumn, array(col(keyColumn)))
        .withColumn("has_osm", lit(true))
    }
  }
}
