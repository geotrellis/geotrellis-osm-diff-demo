import sbt._

object Dependencies {
  val decline         = "com.monovore"                %% "decline"          % "0.5.0"
  val geotrellisSpark = "org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis
  val geotrellisS3    = "org.locationtech.geotrellis" %% "geotrellis-s3"    % Version.geotrellis
  val sparkCore       = "org.apache.spark"            %% "spark-core"       % Version.spark
}
