/**
  * This file implements a VectorPipe Pipeline to generate a vector tile layer of MSFT
  * building geometries tagged with whether there is a matching building in OSM.
  */
package osmdiff

import java.net.URI

import geotrellis.vector.{Feature, Geometry, Point}
import geotrellis.vectortile.VBool
import org.apache.spark.sql.Row
import org.locationtech.jts.{geom => jts}
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
