package oiosmdiff

import java.net.URI

import geotrellis.spark.io.kryo.KryoRegistrator

import com.monovore.decline._

import org.locationtech.geomesa.spark.jts._

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import cats.implicits._

import scala.util.Properties

object Main
    extends CommandApp(
      name = "oi-osm-diff",
      header = "Diffs OI derived road data with OSM",
      main = {
        val osmOrcUriOpt =
          Opts
            .argument[URI]("osmOrcUri")
            .validate("oiGeoJsonUri must be an S3 or file Uri") { uri =>
              uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file") }
            .validate("osmOrcUri must be an .orc file") { _.getPath.endsWith(".orc") }
        val oiGeoJsonUriOpt =
          Opts
            .argument[URI]("oiGeoJsonUri")
            .validate("oiGeoJsonUri must be an S3 or file Uri") { uri =>
              uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file") }
            .validate("oiGeoJsonUri must be a .geojson file") { _.getPath.endsWith(".geojson") }
        val outputS3PrefixOpt =
          Opts
            .argument[URI]("outputS3PathPrefix")
            .validate("outputS3PathPrefix must be an S3 Uri") { _.getScheme.startsWith("s3") }

        (osmOrcUriOpt, oiGeoJsonUriOpt, outputS3PrefixOpt).mapN {
          (osmOrcUri, oiGeoJsonUri, outputS3Prefix) =>
            val conf =
              new SparkConf()
                .setIfMissing("spark.master", "local[*]")
                .setAppName("OI OSM Diff")
                .set("spark.driver.memory", "2g")
                .set("spark.serializer", classOf[KryoSerializer].getName)
                .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
                .set("spark.executorEnv.AWS_REGION", "us-east-1")
                .set("spark.executorEnv.AWS_PROFILE", Properties.envOrElse("AWS_PROFILE", "default"))

            implicit val ss =
              SparkSession
                .builder
                .config(conf)
                .getOrCreate
                .withJTS

            try {
              val vectorDiff = new OiOsmDiff(osmOrcUri, oiGeoJsonUri, outputS3Prefix)
              vectorDiff.saveOiTiles
              vectorDiff.saveOsmTiles
              vectorDiff.saveDiffTiles
            } catch {
              case e: Exception => throw e
            } finally {
              ss.stop
            }
        }
      }
    )
