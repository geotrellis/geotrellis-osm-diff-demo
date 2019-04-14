/**
  * This file implements a VectorPipe Pipeline to generate a vector tile layer of MSFT
  * building geometries tagged with whether there is a matching building in OSM.
  *
  * The comparison is done in Spark DataFrames by joining the two datasets on whether
  * a MSFT building geometry contains an OSM building centroid. While not a perfect
  * comparison, it serves well enough to demo the capabilities of the DataFrame and
  * VectorPipe APIs.
  */
package oiosmdiff

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.vector.{Feature, Geometry, Point}
import geotrellis.vectortile.{VBool, Value}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.jts.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.{geom => jts}
import vectorpipe.{OSM, VectorPipe}
import vectorpipe.functions.osm._
import vectorpipe.internal.WayType
import vectorpipe.vectortile.{Pipeline, VectorTileFeature}

case class BuildingsDiffPipeline(geometryColumn: String, baseOutputURI: URI, gridResolution: Int)
    extends Pipeline {

  override def pack(row: Row, zoom: Int): VectorTileFeature[Geometry] = {
    val hasOsm                = row.getAs[Boolean]("has_osm")
    val properties            = Map("hasOsm" -> VBool(hasOsm))
    val jtsGeom: jts.Geometry = row.getAs[jts.Geometry](geometryColumn)
    if (zoom > 11) {
      Feature(Geometry(jtsGeom), properties)
    } else {
      Feature(Point(jtsGeom.getCentroid), properties)
    }
  }
}

// Stub used for testing the input GeoJson and OSM data frames
case class NoPropsPipeline(geometryColumn: String, baseOutputURI: URI, gridResolution: Int)
    extends Pipeline {

  override def pack(row: Row, zoom: Int): VectorTileFeature[Geometry] = {
    val geom: jts.Geometry = row.getAs[jts.Geometry](geometryColumn)
    Feature(Geometry(geom), Map.empty[String, Value])
  }
}

class BuildingsDiff(osmOrcUri: URI, geoJsonUri: URI, outputS3Prefix: URI)(
    @transient implicit val ss: SparkSession)
    extends LazyLogging
    with Serializable {

  lazy val geoJsonDf: DataFrame = {
    val msftBuildingSchema = StructType(StructField("geometry", GeometryUDT) :: Nil)
    val rdd: RDD[Row] = ss.sparkContext
      .parallelize(Seq(geoJsonUri))
      .flatMap(uri => GeoJsonFeature.readFromGeoJson(uri, "geojson"))
      .map(f => Row(f.geom.jtsGeom))
    ss.createDataFrame(rdd, msftBuildingSchema)
  }

  lazy val osmDf: DataFrame = {
    val osmData = OSM.toGeometry(ss.read.orc(osmOrcUri.toString))
    val osmRoadData = osmData
      .select("id", "_type", "geom", "tags")

    val osmRoads = osmRoadData
      .filter(isBuilding(col("tags")))
      .where(osmRoadData("geom").isNotNull &&
        osmRoadData("_type") === WayType)

    osmRoads.where(st_isValid(col("geom")))
  }

  lazy val joinedDf: DataFrame = {
    val osmWithCentroid: DataFrame = osmDf
      .withColumnRenamed("geom", "osm_geometry")
      .withColumn("osm_centroid", st_centroid(col("osm_geometry")))
      .persist(StorageLevel.MEMORY_AND_DISK)
    val geoJsonDfRenamed = geoJsonDf
      .withColumnRenamed("geometry", "msft_geometry")
      .persist(StorageLevel.MEMORY_AND_DISK)
    geoJsonDfRenamed
      .join(osmWithCentroid, st_contains(col("msft_geometry"), col("osm_centroid")), "left")
      .withColumn("has_osm", st_isValid(col("osm_geometry")))
  }

  def makeTiles: Unit = {
    // Joined DF export
    val pipeline = BuildingsDiffPipeline("msft_geometry", outputS3Prefix, 16)
    VectorPipe(joinedDf,
               pipeline,
               "msft_buildings",
               VectorPipe.Options(12, Some(10), LatLng, Some(WebMercator)))

    // Test OSM or MSFT Buildings DF export
//    val options = VectorPipe.Options(12, None, LatLng, Some(WebMercator))
//    val msftPipeline = NoPropsPipeline("geometry", URI.create(outputS3Prefix.toString + "/msft"), 16)
//    VectorPipe(geoJsonDf, msftPipeline, "geometry", options)
//    val osmPipeline = NoPropsPipeline("geom", URI.create(outputS3Prefix.toString + "/osm"), 16)
//    VectorPipe(osmDf, osmPipeline, "geom", options)
  }
}
