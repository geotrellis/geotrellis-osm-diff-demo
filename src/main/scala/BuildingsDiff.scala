package osmdiff

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector.{Geometry, MultiPolygonResult, Polygon, PolygonResult}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.jts.GeometryUDT
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom.{Geometry => JTSGeometry}
import vectorpipe.{OSM, VectorPipe}
import vectorpipe.functions.osm._
import vectorpipe.internal.WayType

import scala.collection.mutable.ListBuffer

object OutputFormat extends Enumeration {
  type OutputFormat = Value
  val S3, GeoJson, Orc = Value
}

object Source extends Enumeration {
  type Source = Value
  val diff, bing, osm = Value
}

class BuildingsDiff(osmOrcUri: URI, geoJsonUris: Seq[URI], numPartitions: Int)(
    @transient implicit val ss: SparkSession)
    extends LazyLogging
    with Serializable {

  val maxZoom      = 12
  val layoutScheme = ZoomedLayoutScheme(WebMercator)
  val layout       = layoutScheme.levelForZoom(maxZoom).layout
  val partitioner  = new HashPartitioner(numPartitions)

  lazy val geoJsonRdd: RDD[(SpatialKey, Iterable[FeatureWithId])] = {
    ss.sparkContext
      .parallelize(geoJsonUris)
      .flatMap(uri => FeatureWithId.readFromGeoJson(uri, "bing", LatLng, WebMercator))
      .flatMap(feature => {
        val keys = layout.mapTransform.keysForGeometry(feature.geom)
        keys.map(k => (k, feature))
      })
      .partitionBy(partitioner)
      .groupByKey(partitioner)
  }

  lazy val osmRdd: RDD[(SpatialKey, Iterable[FeatureWithId])] = {

    def getTagWithDefault(tagColumn: Column, tagName: String, default: Any): Column = {
      coalesce(tagColumn.getItem(tagName), lit(default))
    }

    val osmData = ss.read.orc(osmOrcUri.toString)
    val osmDataAsGeoms = OSM
      .toGeometry(osmData)
      .select("id", "_type", "geom", "tags")

    val osmBuildings = osmDataAsGeoms
      .filter(isBuilding(col("tags")))
      .where(osmDataAsGeoms("geom").isNotNull &&
        osmDataAsGeoms("_type") === WayType)
      .withColumn("building_type", getTagWithDefault(col("tags"), "building", ""))
      .withColumn("name", getTagWithDefault(col("tags"), "name", ""))
      .drop("tags")

    val processedOsm = osmBuildings.where(st_isValid(col("geom")))
    processedOsm.rdd
      .map { row =>
        val geom = row.getAs[JTSGeometry]("geom")
        geom.normalize

        val buildingType = row.getAs[String]("building_type")
        val name         = row.getAs[String]("name")
        // Workaround for the OSMesa geojson writer not properly escaping double quotes in strings,
        //  which utterly flummoxes Tippecanoe
        val cleanName = name.replace("\"", "'")

        val reprojectedGeom = Geometry(geom).reproject(LatLng, WebMercator)
        FeatureWithId(reprojectedGeom,
                      Map("building_type" -> buildingType, "name" -> cleanName, "source" -> "osm"))
      }
      .flatMap { feature =>
        val keys = layout.mapTransform.keysForGeometry(feature.geom)
        keys.map(k => (k, feature))
      }
      .partitionBy(partitioner)
      .groupByKey(partitioner)
  }

  lazy val diffRdd: RDD[(SpatialKey, Iterable[FeatureWithId])] = {
    val joinedByKeyRdd = geoJsonRdd.fullOuterJoin(osmRdd, partitioner)
    joinedByKeyRdd.mapValues {
      case (Some(msftBuildings), Some(osmBuildings)) => {
        val msftSet = msftBuildings.toSet
        val osmSet  = osmBuildings.toSet

        val osmPossibleMatches  = scala.collection.mutable.Set[FeatureWithId]()
        val msftPossibleMatches = scala.collection.mutable.Set[FeatureWithId]()

        // First limit comparisons to only geometries that could possibly match
        msftSet.foreach { msftBuilding =>
          osmSet.foreach { osmBuilding =>
            if (msftBuilding.geom.intersects(osmBuilding.geom)) {
              osmPossibleMatches += osmBuilding
              msftPossibleMatches += msftBuilding
            }
          }
        }
        // Construct feature sets where there are no matches
        val osmNoMatches = (osmSet -- osmPossibleMatches).toList
        val msftNoMatches = (msftSet -- msftPossibleMatches).toList

        // Loop osm buildings and check if any bing footprint contains the osm centroid. If so, a match!
        // Note that this algorithm implies a one to many relationship for bing to osm buildings. One
        // bing building can match many osm buildings. Which is fine because in most cases the bing
        // satellite detected polygon is larger and less precise than the actual OSM building.
        val msftPossibleMatchesList  = msftPossibleMatches.toList
        val matches = new ListBuffer[FeatureWithId]()
        osmPossibleMatches.foreach { osmFeature =>
          val isMatched = msftPossibleMatchesList.exists { msftFeature =>
            msftFeature.geom.jtsGeom.contains(osmFeature.geom.jtsGeom.getCentroid)
          }
          if (isMatched) {
            matches += FeatureWithId(osmFeature.id, osmFeature.geom, osmFeature.data ++ Map("source" -> "both"))
          }
        }

        // Remove successful matches in last step from consideration
        osmPossibleMatches --= matches

        // Attempt 1 to 1 area intersection ratio check with remaining geometries
        osmPossibleMatches.foreach { osmFeature =>
          val isMatched = msftPossibleMatchesList.exists { msftFeature =>
            (msftFeature.geom.as[Polygon], osmFeature.geom.as[Polygon]) match {
              case (Some(msftPoly), Some(osmPoly)) => {
                if (msftPoly.isValid && osmPoly.isValid) {
                  val AREA_THRESHOLD = 0.75
                  msftFeature.geom.intersection(osmFeature.geom) match {
                    case PolygonResult(p) => if (p.area / msftPoly.area > AREA_THRESHOLD) true else false
                    case MultiPolygonResult(p) => if (p.area / msftPoly.area > AREA_THRESHOLD) true else false
                    case _ => false
                  }
                } else {
                  false
                }
              }
              case _ => false
            }
          }
          if (isMatched) {
            matches += FeatureWithId(osmFeature.id, osmFeature.geom, osmFeature.data ++ Map("source" -> "both"))
          }
        }

        // Return all output plus any remaining unmatched OSM geometries
        msftNoMatches ++ osmNoMatches ++ matches
      }
      case (Some(msftBuildings), None) => msftBuildings
      case (None, Some(osmBuildings))  => osmBuildings
      case _                           => Iterable.empty
    }
    .partitionBy(partitioner)
  }

  def write(source: Source.Value, uri: URI, format: OutputFormat.Value): Unit = {
    val rdd = source match {
      case Source.diff => diffRdd
      case Source.osm => osmRdd
      case Source.bing => geoJsonRdd
    }
    format match {
      case OutputFormat.S3      => makeTiles(rdd, uri)
      case OutputFormat.GeoJson => makeGeoJson(rdd, uri)
      case OutputFormat.Orc     => makeOrc(rdd, uri)
    }
  }

  private def makeOrc(rdd: RDD[(SpatialKey, Iterable[FeatureWithId])], outputPath: URI): Unit = {
    toDataFrame(rdd).write.mode(SaveMode.Overwrite).orc(outputPath.toString)
  }

  private def makeGeoJson(rdd: RDD[(SpatialKey, Iterable[FeatureWithId])], outputPath: URI, geomColIndex: Int = 0): Unit = {
    import org.locationtech.geomesa.spark.jts.util.GeoJSONExtensions._
    toDataFrame(rdd).toGeoJSON(geomColIndex).write.mode(SaveMode.Overwrite).text(outputPath.toString)
  }

  private def makeTiles(rdd: RDD[(SpatialKey, Iterable[FeatureWithId])], outputS3Prefix: URI): Unit = {

    // Export as vector tiles via VectorPipe
    val gridResolution = 4096
    val pipeline       = BuildingsDiffPipeline("geometry", outputS3Prefix, gridResolution)
    VectorPipe(toDataFrame(rdd).withColumn("weight", lit(1)),
               pipeline,
               VectorPipe.Options(maxZoom, Some(4), WebMercator, None, true, gridResolution))
  }

  private def toDataFrame(rdd: RDD[(SpatialKey, Iterable[FeatureWithId])]): DataFrame = {
    // TODO: Dedupe geometries that might exist across multiple SpatialKeys?
    val rowRdd: RDD[Row] = rdd.map(_._2).flatMap(identity).map { feature =>
      val source: String = feature.data.get("source") match {
        case Some(v: String) => v
        case _               => ""
      }
      val name = feature.data.get("name") match {
        case Some(v: String) => v
        case _               => ""
      }
      val buildingType = feature.data.get("building_type") match {
        case Some(v: String) => v
        case _               => ""
      }

      Row(feature.geom.jtsGeom, source, name, buildingType)
    }

    // Flatten into RDD[Row] so we can assign field names to the Row columns and be explicit about type
    val rowSchema = StructType(
      StructField("geometry", GeometryUDT) ::
        StructField("source", StringType) ::
        StructField("name", StringType) ::
        StructField("buildingType", StringType) :: Nil)
    ss.createDataFrame(rowRdd, rowSchema)
  }
}
