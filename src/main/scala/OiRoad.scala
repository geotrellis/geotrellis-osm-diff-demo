package oiosmdiff

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.net.URI
import java.security.InvalidParameterException

import com.typesafe.scalalogging.LazyLogging
import geotrellis.vector.{Feature, Geometry, MultiPolygon}
import geotrellis.vector.io._
import geotrellis.vectortile.VString
import spray.json.JsonReader
import spray.json.JsonParser.ParsingException
import vectorpipe.GenerateVT.VTF

import scala.collection.JavaConverters._

final case class OiRoad(id: String, geom: MultiPolygon) {
  def toVectorTileFeature: VTF[Geometry] = {
    Feature(geom, Map("tileId" -> VString(id), "source" -> VString("oi")))
  }
}

object OiRoad extends Serializable with LazyLogging {
  def readFromGeoJson(uri: URI): Iterator[OiRoad] = {
    val inputStream = uri.getPath match {
      case p if p.endsWith(".geojson") || p.endsWith(".json") => {
        new FileInputStream(uri.toString.stripPrefix("file://"))
      }
      case _ => throw new InvalidParameterException(s"File must be geojson: ${uri.toString}")
    }

    import spray.json.DefaultJsonProtocol._
    final case class OiProperties(tile_id: String, date_part: Int)
    implicit val oiPropertiesJsonReader: JsonReader[OiProperties] = jsonFormat2(OiProperties)

    // Using streaming reads to avoid exploding heap, these files can be big
    // Requires that the GeoJSON input be formatted as a single line per feature
    var invalidCount = 0
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(inputStream))
    reader.lines.iterator.asScala.flatMap { jsonString =>
      try {
        val feature = jsonString.stripSuffix(",").parseGeoJson[Feature[MultiPolygon, OiProperties]]
        if (feature.isValid) {
          Some(OiRoad(feature.data.tile_id, feature.geom))
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
