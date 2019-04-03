package oiosmdiff

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4._
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Geometry => JTSGeometry, GeometryCollection => JTSGeometryCollection}
import vectorpipe._
import vectorpipe.GenerateVT.VTF
import vectorpipe.functions.osm._

class OiOsmDiff(
    osmOrcUri: URI,
    oiGeoJsonUri: URI,
    outputS3Prefix: URI,
    zoom: Int = 12
)(@transient implicit val ss: SparkSession)
    extends LazyLogging
    with Serializable {

  val layoutScheme = ZoomedLayoutScheme(WebMercator)
  val layout       = layoutScheme.levelForZoom(zoom).layout
  val partitioner  = new HashPartitioner(partitions = 64)

  lazy val oiGeoJsonRdd: RDD[(SpatialKey, Iterable[VTF[Geometry]])] = {
    ss.sparkContext
      .parallelize(Seq(oiGeoJsonUri))
      .flatMap(uri => OiRoad.readFromGeoJson(uri))
      .map(oiRoad => OiRoad(oiRoad.id, oiRoad.geom.reproject(LatLng, WebMercator)))
      .flatMap(oiRoad => {
        val keys = layout.mapTransform.keysForGeometry(oiRoad.geom)
        keys.map(k => (k, oiRoad.toVectorTileFeature))
      })
      .partitionBy(partitioner)
      .groupByKey(partitioner)
  }

  def saveOiTiles: Unit = {
    val layerName     = "oi-roads"
    val bucket        = outputS3Prefix.getHost
    val path          = String.join("/", outputS3Prefix.getPath.stripPrefix("/"), layerName)
    val vectorTileRdd = Util.makeVectorTileRDD(oiGeoJsonRdd, layerName, zoom)
    GenerateVT.save(vectorTileRdd, zoom, bucket, path)
  }

  lazy val osmRoadsRdd: RDD[(SpatialKey, Iterable[VTF[Geometry]])] = {
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

    val osmPerTileRdd = validOsmRoads.rdd
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
            "roadType"    -> VString(roadType),
            "surfaceType" -> VString(surfaceType),
            "source"      -> VString("osm")
          )
        Feature(reprojectedGeom, featureInfo)
      }

    osmPerTileRdd
      .flatMap { feature =>
        val keys = layout.mapTransform.keysForGeometry(feature.geom)
        keys.map(k => (k, feature))
      }
      .partitionBy(partitioner)
      .groupByKey(partitioner)
  }

  def saveOsmTiles: Unit = {
    val layerName     = "osm-roads"
    val bucket        = outputS3Prefix.getHost
    val path          = String.join("/", outputS3Prefix.getPath.stripPrefix("/"), layerName)
    val vectorTileRDD = Util.makeVectorTileRDD(osmRoadsRdd, layerName, zoom)
    GenerateVT.save(vectorTileRDD, zoom, bucket, path)
  }

  lazy val diffRdd: RDD[(SpatialKey, Iterable[VTF[Geometry]])] = {

    val oiUnionByKeyRdd: RDD[(SpatialKey, VTF[Geometry])] = oiGeoJsonRdd.flatMapValues { features =>
      val collection = GeometryCollection(features.map(_.geom))
      val multiPolygons: Seq[MultiPolygon] = collection.getAll[MultiPolygon]
      val polygons: Seq[Polygon] = collection.getAll[Polygon]

      (multiPolygons ++ Seq(MultiPolygon(polygons))).unionGeometries.asMultiPolygon match {
        case Some(geom) => Some(Feature(geom, Map.empty[String, Value]))
        case None => None
      }
    }

    val osmUnionByKeyRdd: RDD[(SpatialKey, VTF[Geometry])] = osmRoadsRdd.flatMapValues { features =>
      val collection = GeometryCollection(features.map(_.geom))
      val multiLines: Seq[MultiLine] = collection.getAll[MultiLine]
      val lines: Seq[Line] = collection.getAll[Line]
      (multiLines ++ Seq(MultiLine(lines))).unionGeometries.asMultiLine match {
        case Some(geom) => Some(Feature(geom, Map.empty[String, Value]))
        case None => None
      }
    }

    // Left outer join to persist a tile with osm features that has no oi features since this is
    // effectively the same operation as osm - oi where oi is an empty feature set.
    val geomsByKeyRdd: RDD[(SpatialKey, Iterable[(VTF[Geometry], Option[VTF[Geometry]])])] =
      osmUnionByKeyRdd.leftOuterJoin(oiUnionByKeyRdd, partitioner).groupByKey

    geomsByKeyRdd.flatMapValues { geomPairs =>
      geomPairs.headOption match {
        case Some((osmFeature, None)) => Some(Seq(osmFeature))
        case Some((osmFeature, maybeOiFeature)) => {
          val osmGeom = osmFeature.geom.jtsGeom
          val oiGeom  = maybeOiFeature.get.geom.jtsGeom
          val diff    = osmGeom.difference(oiGeom)
          if (diff.isValid && !diff.isEmpty) {
            Some(Seq(Feature(Geometry(diff), Map.empty[String, Value])))
          } else {
            None
          }
        }
        case _ => None
      }
    }
  }

  def saveDiffTiles: Unit = {
    val layerName     = "osm-diff"
    val bucket        = outputS3Prefix.getHost
    val path          = String.join("/", outputS3Prefix.getPath.stripPrefix("/"), layerName)
    val vectorTileRDD = Util.makeVectorTileRDD(diffRdd, layerName, zoom)
    GenerateVT.save(vectorTileRDD, zoom, bucket, path)
  }
}
