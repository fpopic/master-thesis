lazy val root = (project in file(".")).settings(
  name := "spark-recommender",
  version := "1.0",
  scalaVersion := "2.11.8",
  organization := "Faculty of Electrical Engineering and Computing, University of Zagreb",
  startYear := Some(2017),
  mainClass in Compile := Some("hr.fer.ztel.thesis.Main"),
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  parallelExecution in Test := true,
  test in assembly := {} // remove if you want to start tests
)

lazy val unprovidedDependencies = Seq(
//  "com.github.fommil.netlib" % "all" % "1.1.2"
//  "org.scalactic" %% "scalactic" % "3.0.1",
//  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

libraryDependencies ++= unprovidedDependencies

val sparkVersion = "2.1.0.cloudera1"

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

libraryDependencies ++= providedDependencies.map(_ % "provided")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case _ => MergeStrategy.first
}

resolvers ++= Seq(
  "Cloudera Maven repository" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)
