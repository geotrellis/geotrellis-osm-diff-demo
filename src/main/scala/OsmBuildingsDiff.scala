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
import org.locationtech.jts.geom.{Geometry => JTSGeometry}
import vectorpipe._
import vectorpipe.internal.WayType
import vectorpipe.GenerateVT.VTF
import vectorpipe.functions.osm._

class MsftOsmDiff(
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

  lazy val geoJsonRdd: RDD[(SpatialKey, Iterable[VTF[Geometry]])] = {
    ss.sparkContext
      .parallelize(Seq(oiGeoJsonUri))
      .flatMap(uri => GeoJsonFeature.readFromGeoJson(uri, "geojson"))
      .map(f => GeoJsonFeature(f.id, f.source, f.geom.reproject(LatLng, WebMercator)))
      .flatMap(f => {
        val keys = layout.mapTransform.keysForGeometry(f.geom)
        keys.map(k => (k, f.toVectorTileFeature))
      })
      .partitionBy(partitioner)
      .groupByKey(partitioner)
  }

  def saveGeoJsonTiles: Unit = {
    val layerName     = "geojson"
    val bucket        = outputS3Prefix.getHost
    val path          = String.join("/", outputS3Prefix.getPath.stripPrefix("/"), layerName)
    val vectorTileRdd = Util.makeVectorTileRDD(geoJsonRdd, layerName, zoom)
    GenerateVT.save(vectorTileRdd, zoom, bucket, path)
  }

  lazy val osmRdd: RDD[(SpatialKey, Iterable[VTF[Geometry]])] = {
    val isValid: (JTSGeometry) => Boolean = (jtsGeom: JTSGeometry) => jtsGeom.isValid()
    val isValidUDF                        = udf(isValid)

    val osmData = OSM.toGeometry(ss.read.orc(osmOrcUri.toString))
    val osmRoadData = osmData
      .select("id", "_type", "geom", "tags")
      .withColumn("buildingType", osmData("tags").getField("building"))

    val osmRoads = osmRoadData
      .filter(isBuilding(col("tags")))
      .where(osmRoadData("geom").isNotNull &&
        osmRoadData("_type") === WayType)

    val validOsmRoads = osmRoads.where(isValidUDF(osmRoadData("geom")))

    val osmPerTileRdd = validOsmRoads.rdd
      .map { row =>
        val id = row.getAs[Long]("id")

        val buildingType =
          row.getAs[String]("buildingType") match {
            case null      => "null"
            case s: String => s
          }

        val geom            = row.getAs[JTSGeometry]("geom")
        val reprojectedGeom = Geometry(geom).reproject(LatLng, WebMercator)

        val featureInfo: Map[String, Value] =
          Map(
            "id"           -> VString(id.toString),
            "buildingType" -> VString(buildingType),
            "source"       -> VString("osm")
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
    val layerName     = "osm"
    val bucket        = outputS3Prefix.getHost
    val path          = String.join("/", outputS3Prefix.getPath.stripPrefix("/"), layerName)
    val vectorTileRDD = Util.makeVectorTileRDD(osmRdd, layerName, zoom)
    GenerateVT.save(vectorTileRDD, zoom, bucket, path)
  }

  lazy val diffRdd: RDD[(SpatialKey, Iterable[VTF[Geometry]])] = {

    def unionMultiPolygonFeatures(features: Iterable[VTF[Geometry]]): Traversable[VTF[Geometry]] = {
      val collection                       = GeometryCollection(features.map(_.geom))
      val multiPolygons: Seq[MultiPolygon] = collection.getAll[MultiPolygon]
      val polygons: Seq[Polygon]           = collection.getAll[Polygon]

      (multiPolygons ++ Seq(MultiPolygon(polygons))).unionGeometries.asMultiPolygon match {
        case Some(geom) => Some(Feature(geom, Map.empty[String, Value]))
        case None       => None
      }
    }

    val geoJsonUnionByKeyRdd: RDD[(SpatialKey, VTF[Geometry])] =
      geoJsonRdd.flatMapValues(unionMultiPolygonFeatures)
    val osmUnionByKeyRdd: RDD[(SpatialKey, VTF[Geometry])] =
      osmRdd.flatMapValues(unionMultiPolygonFeatures)

    // Left outer join to persist a tile with osm features that has no oi features since this is
    // effectively the same operation as osm - oi where oi is an empty feature set.
    val geomsByKeyRdd: RDD[(SpatialKey, Iterable[(VTF[Geometry], Option[VTF[Geometry]])])] =
      osmUnionByKeyRdd.leftOuterJoin(geoJsonUnionByKeyRdd, partitioner).groupByKey

    geomsByKeyRdd.flatMapValues { geomPairs =>
      geomPairs.headOption match {
        case Some((osmFeature, None)) => Some(Seq(osmFeature))
        case Some((osmFeature, maybeGeoJsonFeature)) => {
          val osmGeom = osmFeature.geom.jtsGeom
          val oiGeom  = maybeGeoJsonFeature.get.geom.jtsGeom
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
