import sbtlighter._

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
    case "reference.conf" => MergeStrategy.concat
    case "application.conf" => MergeStrategy.concat
    case PathList("META-INF", xs@_*) =>
      xs match {
        case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
        // Concatenate everything in the services directory to keep GeoTools happy.
        case ("services" :: _ :: Nil) =>
          MergeStrategy.concat
        // Concatenate these to keep JAI happy.
        case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
          MergeStrategy.concat
        case (name :: Nil) => {
          // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
          if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF"))
            MergeStrategy.discard
          else
            MergeStrategy.first
        }
        case _ => MergeStrategy.first
      }
    case _ => MergeStrategy.first
  },
  // SBT Lighter Spark settings
  // TODO: Fill out this configuration. What do we need to keep from us-buildings or SDG roads?
  sparkAwsRegion := "us-east-1",
  sparkClusterName := "osm-diff",
  sparkCoreEbsSize := None,
  sparkCorePrice := Some(0.5),
  sparkCoreType := "m5d.2xlarge",
  sparkEmrApplications := Seq("Spark", "Zeppelin", "Ganglia"),
  sparkEmrRelease := "emr-5.16.0",
  sparkEmrServiceRole := "EMR_DefaultRole",
  sparkInstanceCount := 21,
  sparkInstanceRole := "EMR_EC2_DefaultRole",
  sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("geotrellis-emr"),
  sparkMasterEbsSize := None,
  sparkMasterPrice := Some(0.5),
  sparkMasterType := "m5d.2xlarge",
  sparkS3JarFolder := "s3://geotrellis-test/oi-osm-diff/jars",
  sparkS3LogUri := Some("s3://geotrellis-test/oi-osm-diff/logs"),
  sparkEmrConfigs := List(
    EmrConfig("spark").withProperties(
      "maximizeResourceAllocation" -> "true"
    ),
    EmrConfig("spark-defaults").withProperties(
      "spark.driver.maxResultSize"        -> "8G",
      "spark.dynamicAllocation.enabled"   -> "true",
      "spark.shuffle.service.enabled"     -> "true",
      "spark.shuffle.compress"            -> "true",
      "spark.shuffle.spill.compress"      -> "true",
      "spark.rdd.compress"                -> "true",
      "spark.executor.extraJavaOptions"   -> "-XX:+UseParallelGC -Dgeotrellis.s3.threads.rdd.write=64",
    ),
    EmrConfig("yarn-site").withProperties(
      "yarn.resourcemanager.am.max-attempts" -> "1",
      "yarn.nodemanager.vmem-check-enabled"  -> "false",
      "yarn.nodemanager.pmem-check-enabled"  -> "false"
    )
  )
)
