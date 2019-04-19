package osmdiff

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.net.{URI, URL}
import java.security.InvalidParameterException
import java.util.UUID.randomUUID
import java.util.zip.ZipInputStream

import com.typesafe.scalalogging.LazyLogging
import geotrellis.vector.{Feature, Geometry}
import geotrellis.vector.io._
import geotrellis.vectortile.VString
import spray.json.JsonReader
import spray.json.JsonParser.ParsingException
import vectorpipe.vectortile.VectorTileFeature

import scala.collection.JavaConverters._

case class GeoJsonFeature(id: String, source: String, geom: Geometry) {
  def toVectorTileFeature: VectorTileFeature[Geometry] = {
    Feature(geom, Map("tileId" -> VString(id), "source" -> VString(source)))
  }
}

object GeoJsonFeature extends Serializable with LazyLogging {
  def readFromGeoJson(uri: URI, sourceName: String): Iterator[GeoJsonFeature] = {
    val inputStream = uri.getPath match {
      case p if p.endsWith(".geojson") || p.endsWith(".json") => {
        uri.toURL.openStream
      }
      case p if p.endsWith(".zip") => {
        val zip = new ZipInputStream(uri.toURL.openStream)
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
          Some(GeoJsonFeature(randomUUID().toString, sourceName, feature.geom))
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
