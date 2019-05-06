import sbt._

object Dependencies {
  val decline              = "com.monovore"                %% "decline"               % "0.5.0"
  val geomesaSparkSql      = "org.locationtech.geomesa"    %% "geomesa-spark-sql"     % "2.3.0"
  val geotrellisSpark      = "org.locationtech.geotrellis" %% "geotrellis-spark"      % Version.geotrellis
  val geotrellisS3         = "org.locationtech.geotrellis" %% "geotrellis-s3"         % Version.geotrellis
  val geotrellisVectorTile = "org.locationtech.geotrellis" %% "geotrellis-vectortile" % Version.geotrellis
  val scalaLogging         = "com.typesafe.scala-logging"  %% "scala-logging"         % "3.9.0"
  val sparkCore            = "org.apache.spark"            %% "spark-core"            % Version.spark
  val sparkHive            = "org.apache.spark"            %% "spark-hive"            % Version.spark
  val sparkSql             = "org.apache.spark"            %% "spark-sql"             % Version.spark
  val vectorpipe           = "com.azavea"                  %% "vectorpipe"            % "1.0.0-RC3"
}
