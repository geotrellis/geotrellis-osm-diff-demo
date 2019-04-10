package oiosmdiff

import geotrellis.proj4.WebMercator
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector._
import geotrellis.vectortile.{StrictLayer, Value, VectorTile}
import org.apache.spark.rdd.RDD
import vectorpipe.GenerateVT.VTF

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object Util {

  def getAllOf[G <: Geometry](features: Iterable[Feature[Geometry, Map[String, Value]]])(
      implicit c: ClassTag[G]): Iterable[Feature[G, Map[String, Value]]] = {
    features.flatMap { feature => feature.geom.as[G].map(geom => Feature(geom, feature.data))}
  }

  def makeVectorTileRDD(inputRdd: RDD[(SpatialKey, Iterable[VTF[Geometry]])],
                        layerName: String,
                        zoom: Int): RDD[(SpatialKey, VectorTile)] = {
    val layoutScheme = ZoomedLayoutScheme(WebMercator)
    val layout       = layoutScheme.levelForZoom(zoom).layout

    inputRdd.map {
      case (key, features) =>
        val extent = layout.mapTransform.keyToExtent(key)

        val layer = StrictLayer(
          layerName,
          tileWidth = 4096,
          version = 2,
          tileExtent = extent,
          Util.getAllOf[Point](features).toSeq,
          Util.getAllOf[MultiPoint](features).toSeq,
          Util.getAllOf[Line](features).toSeq,
          Util.getAllOf[MultiLine](features).toSeq,
          Util.getAllOf[Polygon](features).toSeq,
          Util.getAllOf[MultiPolygon](features).toSeq
        )
        (key, VectorTile(Map(layerName -> layer), extent))
    }
  }
}
