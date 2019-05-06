package osmdiff

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector.{Feature, Geometry, PointResult}
import geotrellis.vectortile.{VBool, Value}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.jts.GeometryUDT
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom.{Geometry => JTSGeometry}
import vectorpipe.{OSM, VectorPipe}
import vectorpipe.functions.osm._
import vectorpipe.internal.WayType
import vectorpipe.vectortile.VectorTileFeature

object OutputFormat extends Enumeration {
  type OutputFormat = Value
  val S3, GeoJson = Value
}

class BuildingsDiff(osmOrcUri: URI, geoJsonUris: Seq[URI], numPartitions: Int)(
    @transient implicit val ss: SparkSession)
    extends LazyLogging
    with Serializable {

  val maxZoom      = 12
  val layoutScheme = ZoomedLayoutScheme(WebMercator)
  val layout       = layoutScheme.levelForZoom(maxZoom).layout
  val partitioner  = new HashPartitioner(numPartitions)

  lazy val geoJsonRdd: RDD[(SpatialKey, Iterable[VectorTileFeature[Geometry]])] = {
    ss.sparkContext
      .parallelize(geoJsonUris)
      .flatMap(uri => GeoJsonFeature.readFromGeoJson(uri, "geojson"))
      .map(feature => feature.geom.reproject(LatLng, WebMercator))
      .flatMap(geom => {
        val jtsGeom = geom.jtsGeom
        jtsGeom.normalize
        val normalizedGeom = Geometry(jtsGeom)
        val keys           = layout.mapTransform.keysForGeometry(normalizedGeom)
        keys.map(k => (k, Feature(normalizedGeom, Map.empty[String, Value])))
      })
      .partitionBy(partitioner)
      .groupByKey(partitioner)
  }

  lazy val osmRdd: RDD[(SpatialKey, Iterable[VectorTileFeature[Geometry]])] = {
    val osmData = ss.read.orc(osmOrcUri.toString)
    val osmDataAsGeoms = OSM
      .toGeometry(osmData)
      .select("id", "_type", "geom", "tags")

    val osmBuildings = osmDataAsGeoms
      .filter(isBuilding(col("tags")))
      .where(osmDataAsGeoms("geom").isNotNull &&
        osmDataAsGeoms("_type") === WayType)

    val processedOsm = osmBuildings.where(st_isValid(col("geom")))
    processedOsm.rdd
      .map { row =>
        val geom = row.getAs[JTSGeometry]("geom")
        geom.normalize
        val reprojectedGeom = Geometry(geom).reproject(LatLng, WebMercator)
        Feature(reprojectedGeom, Map.empty[String, Value])
      }
      .flatMap { feature =>
        val keys = layout.mapTransform.keysForGeometry(feature.geom)
        keys.map(k => (k, feature))
      }
      .partitionBy(partitioner)
      .groupByKey(partitioner)
  }

  lazy val diffRdd: RDD[(SpatialKey, Iterable[VectorTileFeature[Geometry]])] = {
    val osmCentroidRdd: RDD[(SpatialKey, Iterable[VectorTileFeature[Geometry]])] =
      osmRdd.mapValues {
        _.flatMap { feature =>
          feature.geom.centroid match {
            case PointResult(geom) => Some(Feature(geom, Map.empty[String, Value]))
            case _                 => None
          }
        }
      }
    val joinedByKeyRdd = geoJsonRdd.leftOuterJoin(osmCentroidRdd, partitioner)
    joinedByKeyRdd.mapValues {
      case (msftBuildings, None) =>
        msftBuildings.map { f =>
          Feature(f.geom, Map("hasOsm" -> VBool(false)))
        }
      case (msftBuildings, Some(osmCentroids)) => {
        msftBuildings.map { feature =>
          val hasOsmMatch = osmCentroids.exists { osmFeature =>
            feature.geom.jtsGeom.contains(osmFeature.geom.jtsGeom)
          }
          Feature(feature.geom, Map("hasOsm" -> VBool(hasOsmMatch)))
        }
      }
      case _ => Iterable.empty
    }
  }

  lazy val diffDataFrame: DataFrame = {
    // TODO: Dedupe geometries that might exist across multiple SpatialKeys?
    val flattenedDiffRdd: RDD[Row] = diffRdd.map(_._2).flatMap(identity).map { feature =>
      val hasOsm: Boolean = feature.data.get("hasOsm") match {
        case Some(v: VBool) => v.value
        case _              => false
      }
      Row(feature.geom.jtsGeom, hasOsm)
    }

    // Flatten into RDD[Row] so we can assign field names to the Row columns and be explicit about type
    val rowSchema = StructType(
      StructField("geometry", GeometryUDT) ::
        StructField("has_osm", BooleanType) :: Nil)
    ss.createDataFrame(flattenedDiffRdd, rowSchema).withColumn("weight", lit(1))
  }

  def write(uri: URI, format: OutputFormat.Value): Unit = {
    format match {
      case OutputFormat.S3 => makeTiles(uri)
      case OutputFormat.GeoJson => makeGeoJson(uri)
    }
  }

  private def makeGeoJson(outputPath: URI): Unit = {
    import org.locationtech.geomesa.spark.jts.util.GeoJSONExtensions._
    diffDataFrame.toGeoJSON(0).write.text(outputPath.toString)
  }

  private def makeTiles(outputS3Prefix: URI): Unit = {

    // Export as vector tiles via VectorPipe
    val gridResolution = 4096
    val pipeline = BuildingsDiffPipeline("geometry", outputS3Prefix, gridResolution)
    VectorPipe(diffDataFrame,
               pipeline,
               VectorPipe.Options(maxZoom, Some(4), WebMercator, None, true, gridResolution))
  }
}