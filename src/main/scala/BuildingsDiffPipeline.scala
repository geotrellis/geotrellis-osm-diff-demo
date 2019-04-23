/**
  * This file implements a VectorPipe Pipeline to generate a vector tile layer of MSFT
  * building geometries tagged with whether there is a matching building in OSM.
  *
  * The comparison is done in Spark DataFrames by joining the two datasets on whether
  * a MSFT building geometry contains an OSM building centroid. While not a perfect
  * comparison, it serves well enough to demo the capabilities of the DataFrame and
  * VectorPipe APIs.
  */
package osmdiff

import java.net.URI

import geotrellis.vector.{Feature, Geometry, Point}
import geotrellis.vectortile.{VBool, Value}
import org.apache.spark.sql.Row
import org.locationtech.jts.{geom => jts}
import vectorpipe.vectortile
import vectorpipe.vectortile.{Pipeline, SingleLayer, VectorTileFeature}

case class BuildingsDiffPipeline(geometryColumn: String, baseOutputURI: URI, gridResolution: Int)
    extends Pipeline {

  val layerMultiplicity = SingleLayer("msft_buildings")

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
