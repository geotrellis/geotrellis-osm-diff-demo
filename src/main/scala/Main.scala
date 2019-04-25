package osmdiff

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
      name = "osm-diff",
      header = "Diffs OI derived road data with OSM",
      main = {
        val osmOrcUriOpt =
          Opts
            .argument[URI]("osmOrcUri")
            .validate("oiGeoJsonUri must be an S3 or file Uri") { uri =>
              uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file")
            }
            .validate("osmOrcUri must be an .orc file") { _.getPath.endsWith(".orc") }
        val outputS3PrefixOpt =
          Opts
            .argument[URI]("outputS3PathPrefix")
            .validate("outputS3PathPrefix must be an S3 Uri") { _.getScheme.startsWith("s3") }
        val buildingsUriOpt =
          Opts
            .option[URI]("buildings", help = "URI to GeoJson (optionally zipped) buildings file")
            .validate("buildings must be an S3, HTTPS or file Uri") { uri =>
              uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("https") || uri.getScheme
                .startsWith("file")
            }
            .validate("buildings must be a .geojson or zip file") { uri =>
              uri.getPath.endsWith(".geojson") || uri.getPath.endsWith(".zip")
            }
        val buildingsAllOpt = Opts
          .flag("all-buildings",
                help = "Read all buildings in US. If not provided, --buildings must be provided")
          .orFalse
        val numPartitionsOpt =
          Opts
            .option[Int]("numPartitions",
                         help = "Number of partitions for Spark HashPartitioner. Defaults to 64.")
            .withDefault(64)

        (osmOrcUriOpt, outputS3PrefixOpt, buildingsUriOpt, buildingsAllOpt, numPartitionsOpt).mapN {
          (osmOrcUri, outputS3Prefix, buildingsUri, buildingsAll, numPartitions) =>
            val conf =
              new SparkConf()
                .setAppName("OSM Diff: MSFT Buildings")
                .setIfMissing("spark.master", "local[*]")
                .setIfMissing("spark.driver.memory", "2g")
                .set("spark.serializer", classOf[KryoSerializer].getName)
                .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
                .set("spark.sql.broadcastTimeout", "600")
                .set("spark.kryoserializer.buffer.max", "1g")
                .set("spark.executorEnv.AWS_REGION", "us-east-1")
                .set("spark.executorEnv.AWS_PROFILE",
                     Properties.envOrElse("AWS_PROFILE", "default"))

            implicit val ss =
              SparkSession.builder
                .config(conf)
                .getOrCreate
                .withJTS

            try {
              val buildingUris: Seq[URI] = if (buildingsAll) {
                USBuilding.geoJsonURLs.map(new URI(_))
              } else {
                Seq(buildingsUri)
              }
              val buildingsDiff =
                new BuildingsDiff(osmOrcUri, buildingUris, outputS3Prefix, numPartitions)
              buildingsDiff.makeTiles
            } catch {
              case e: Exception => throw e
            } finally {
              ss.stop
            }
        }
      }
    )
