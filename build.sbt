lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "geotrellis",
      scalaVersion := Version.scala
    )),
  name := "geotrellis-oi-osm-diff",
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  addCompilerPlugin(scalafixSemanticdb),
  scalacOptions ++= Seq(
    "-Yrangepos",
    "-Ywarn-unused"
  ),
  libraryDependencies ++= Seq(
    Dependencies.decline,
    Dependencies.geotrellisSpark,
    Dependencies.geotrellisS3,
    Dependencies.geotrellisVectorTile,
    Dependencies.vectorpipe,
    Dependencies.scalaLogging,
    Dependencies.sparkCore % "compile",
    Dependencies.sparkHive % "compile",
    Dependencies.sparkSql  % "compile"
  ),
  resolvers ++= Seq(
    "LocationTech Snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
    "LocationTech Releases" at "https://repo.locationtech.org/content/groups/releases",
    Resolver.bintrayRepo("azavea", "geotrellis"),
    Resolver.bintrayRepo("azavea", "maven")
  ),
  // Settings for sbt-assembly plugin which builds fat jars for spark-submit
  assemblyMergeStrategy in assembly := {
    case s if s.startsWith("META-INF/services") => MergeStrategy.concat
    case "reference.conf" | "application.conf"  => MergeStrategy.concat
    case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
    case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
    case _ => MergeStrategy.first
  },
  // SBT Lighter Spark settings
  // TODO: Fill out this configuration. What do we need to keep from us-buildings or SDG roads?
  sparkAwsRegion := "us-east-1",
  sparkClusterName := "oi-osm-diff",
  sparkEmrApplications := Seq("Spark", "Zeppelin"),
  sparkEmrRelease := "emr-5.22.0",
  sparkEmrServiceRole := "EMR_DefaultRole",
  sparkInstanceCount := 5,
  sparkInstanceRole := "EMR_EC2_DefaultRole",
  sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("geotrellis-emr"),
  sparkS3JarFolder := "s3://geotrellis-test/oi-osm-diff/jars",
  sparkS3LogUri := Some("s3://geotrellis-test/oi-osm-diff/logs")
)
