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
    Dependencies.geotrellisSpark,
    Dependencies.geotrellisS3,
    Dependencies.decline,
    Dependencies.sparkCore % "compile"
  ),
  resolvers ++= Seq(
    "LocationTech Snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
    "LocationTech Releases" at "https://repo.locationtech.org/content/groups/releases",
    DefaultMavenRepository,
    Resolver.url("typesafe", url("http://repo.typesafe.com/typesafe/ivy-releases/"))(
      Resolver.ivyStylePatterns)
  ),
  // SBT Lighter Spark settings
  // TODO: Fill out this configuration. What do we need to keep from us-buildings or SDG roads?
  sparkEmrRelease := "emr-5.22.0",
  sparkAwsRegion := "us-east-1",
  sparkS3JarFolder := "s3://geotrellis-test/oi-osm-diff/jars",
  sparkS3LogUri := Some("s3://geotrellis-test/oi-osm-diff/logs")
)
