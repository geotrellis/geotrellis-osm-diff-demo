package osmdiff

import java.util.UUID

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.security.InvalidParameterException
import java.util.zip.ZipInputStream

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.{Feature, Geometry}
import geotrellis.vector.io._
import spray.json.JsonReader
import spray.json.JsonParser.ParsingException

import scala.collection.JavaConverters._

case class FeatureWithId(id: UUID, geom: Geometry, data: Map[String, Any]) {
  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: FeatureWithId => obj.id == id
      case _ => false
    }
  }

  override def hashCode(): Int = id.hashCode
}

object FeatureWithId extends Serializable with LazyLogging {
  def apply(geom: Geometry, data: Map[String, Any]): FeatureWithId =
    FeatureWithId(UUID.randomUUID, geom, data)

  def readFromGeoJson(uri: URI,
                      sourceName: String,
                      fromCrs: CRS = LatLng,
                      toCrs: CRS = LatLng): Iterator[FeatureWithId] = {
    val inputStream = uri.getPath match {
      case p if p.endsWith(".geojson") || p.endsWith(".json") => {
        uri.toURL.openStream
      }
      case p if p.endsWith(".zip") => {
        val zip   = new ZipInputStream(uri.toURL.openStream)
        val entry = zip.getNextEntry
        logger.info(s"Reading: $uri - ${entry.getName}")
        zip
      }
      case _ => throw new InvalidParameterException(s"File must be geojson: ${uri}")
    }

    import spray.json.DefaultJsonProtocol._
    final case class GeoJsonProperties()
    implicit val oiPropertiesJsonReader: JsonReader[GeoJsonProperties] = jsonFormat0(
      GeoJsonProperties)

    // Using streaming reads to avoid exploding heap, these files can be big
    // Requires that the GeoJSON input be formatted as a single line per feature
    var invalidCount           = 0
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(inputStream))
    reader.lines.iterator.asScala.flatMap { jsonString =>
      try {
        val feature = jsonString.stripSuffix(",").parseGeoJson[Feature[Geometry, GeoJsonProperties]]
        if (feature.geom.isValid) {
          val jtsGeom = feature.geom.reproject(fromCrs, toCrs).jtsGeom
          jtsGeom.normalize
          Some(FeatureWithId(Geometry(jtsGeom), Map("source" -> sourceName)))
        } else {
          invalidCount += 1
          logger.info(s"INVALID: ${invalidCount}")
          None
        }
      } catch {
        case _: ParsingException => {
          None
        }
        case e: Exception => throw e
      }
    }
  }
}
