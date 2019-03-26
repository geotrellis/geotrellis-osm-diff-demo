package oiosmdiff

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4._
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector.{Feature, Geometry}
import geotrellis.vectortile._
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.geotools.metadata.iso.SpatialAttributeSupplementImpl
import org.locationtech.jts.geom.{Geometry => JTSGeometry}
import vectorpipe._
import vectorpipe.GenerateVT.VTF
import vectorpipe.functions.osm._

class OiOsmDiff(
    osmOrcUri: URI,
    oiGeoJsonUri: URI
)(@transient implicit val ss: SparkSession)
    extends LazyLogging
    with Serializable {

  val layoutScheme = ZoomedLayoutScheme(WebMercator)


  def saveOiTilesForZoom(zoom: Int, outputS3Prefix: URI): Unit = {

    val partitioner = new HashPartitioner(partitions=64)

    val layout       = layoutScheme.levelForZoom(zoom).layout
    val projectedGeoJsonPerTileRDD: RDD[(SpatialKey, Iterable[OiRoad])] =
      ss.sparkContext
        .parallelize(Seq(oiGeoJsonUri))
        .flatMap(uri => OiRoad.readFromGeoJson(uri))
        .map(oiRoad => OiRoad(oiRoad.id, oiRoad.geom.reproject(LatLng, WebMercator)))
        .flatMap(oiRoad => {
          val keys = layout.mapTransform.keysForGeometry(oiRoad.geom)
          keys.map(k => (k, oiRoad))
        }).partitionBy(partitioner).groupByKey(partitioner)

    val layerName = "oi-roads"
    val vectorTileRDD: RDD[(SpatialKey, VectorTile)] = projectedGeoJsonPerTileRDD.map { case (key, oiRoads) =>
      val extent = layout.mapTransform.keyToExtent(key)
      val layer = StrictLayer(
        name = layerName,
        tileWidth = 4096,
        version = 2,
        tileExtent = extent,
        points = Seq.empty, multiPoints = Seq.empty,
        lines = Seq.empty, multiLines = Seq.empty,
        polygons = Seq.empty,
        multiPolygons = oiRoads.map(_.toVectorTileFeature).toSeq
      )
      (key, VectorTile(Map(layerName -> layer), extent))
    }

    val bucket    = outputS3Prefix.getHost
    val path      = String.join("/", outputS3Prefix.getPath.stripPrefix("/"), layerName)
    GenerateVT.save(vectorTileRDD, zoom, bucket, path)
  }

  def saveOsmTilesForZoom(zoom: Int, outputS3Prefix: URI): Unit = {
    val badRoads = Seq("proposed", "construction", "elevator")

    val osmData                           = OSM.toGeometry(ss.read.orc(osmOrcUri.toString))
    val isValid: (JTSGeometry) => Boolean = (jtsGeom: JTSGeometry) => jtsGeom.isValid()
    val isValidUDF                        = udf(isValid)

    val osmRoadData = osmData
      .select("id", "_type", "geom", "tags")
      .withColumn("roadType", osmData("tags").getField("highway"))
      .withColumn("surfaceType", osmData("tags").getField("surface"))

    val osmRoads = osmRoadData
      .filter(isRoad(col("tags")))
      .where(
        osmRoadData("geom").isNotNull &&
          osmRoadData("_type") === 2 &&
          !osmRoadData("roadType").isin(badRoads: _*))

    val validOsmRoads = osmRoads.where(isValidUDF(osmRoadData("geom")))

    val osmRoadsRDD: RDD[VTF[Geometry]] =
      validOsmRoads.rdd
        .map { row =>
          val id = row.getAs[Long]("id")

          val roadType =
            row.getAs[String]("roadType") match {
              case null      => "null"
              case s: String => s
            }

          val surfaceType =
            row.getAs[String]("surfaceType") match {
              case null      => "null"
              case s: String => s
            }

          val geom            = row.getAs[JTSGeometry]("geom")
          val reprojectedGeom = Geometry(geom).reproject(LatLng, WebMercator)

          val featureInfo: Map[String, Value] =
            Map(
              "id"          -> VString(id.toString),
              "__id"        -> VString(id.toString),
              "roadType"    -> VString(roadType),
              "surfaceType" -> VString(surfaceType)
            )

          Feature(reprojectedGeom, featureInfo)
        }

    val layout       = layoutScheme.levelForZoom(zoom).layout
    val keyedOsmRoadsRDD: RDD[(SpatialKey, (SpatialKey, VTF[Geometry]))] =
      GenerateVT.keyToLayout(osmRoadsRDD, layout)

    val layerName = "osm-roads"
    val bucket    = outputS3Prefix.getHost
    val path      = String.join("/", outputS3Prefix.getPath.stripPrefix("/"), layerName)
    val vectorTiles: RDD[(SpatialKey, VectorTile)] =
      GenerateVT.makeVectorTiles(keyedOsmRoadsRDD, layout, layerName)
    GenerateVT.save(vectorTiles, zoom, bucket, path)
  }
}
