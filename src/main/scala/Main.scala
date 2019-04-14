package oiosmdiff

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.jts._

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
              uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file")
            }
            .validate("osmOrcUri must be an .orc file") { _.getPath.endsWith(".orc") }
        val geoJsonUriOpt =
          Opts
            .argument[URI]("geoJsonUri")
            .validate("geoJsonUri must be an S3, HTTPS or file Uri") { uri =>
              uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("https") || uri.getScheme
                .startsWith("file")
            }
            .validate("oiGeoJsonUri must be a .geojson or zip file") { uri =>
              uri.getPath.endsWith(".geojson") || uri.getPath.endsWith(".zip")
            }
        val outputS3PrefixOpt =
          Opts
            .argument[URI]("outputS3PathPrefix")
            .validate("outputS3PathPrefix must be an S3 Uri") { _.getScheme.startsWith("s3") }

        (osmOrcUriOpt, geoJsonUriOpt, outputS3PrefixOpt).mapN {
          (osmOrcUri, geoJsonUri, outputS3Prefix) =>
            val conf =
              new SparkConf()
                .setAppName("OI OSM Diff")
                .setIfMissing("spark.master", "local[*]")
                .setIfMissing("spark.driver.memory", "2g")
                .set("spark.serializer", classOf[KryoSerializer].getName)
                .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
                .set("spark.sql.crossJoin.enabled", "true")
                .set("spark.sql.broadcastTimeout", "600")
                .set("spark.executorEnv.AWS_REGION", "us-east-1")
                .set("spark.executorEnv.AWS_PROFILE",
                     Properties.envOrElse("AWS_PROFILE", "default"))

            implicit val ss =
              SparkSession.builder
                .config(conf)
                .getOrCreate
                .withJTS

            try {
              val buildingsDiff = new BuildingsDiff(osmOrcUri, geoJsonUri, outputS3Prefix)
              buildingsDiff.makeTiles
            } catch {
              case e: Exception => throw e
            } finally {
              ss.stop
            }
        }
      }
    )
