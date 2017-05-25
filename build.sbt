lazy val root = (project in file(".")).settings(
  name := "spark-recommender",
  version := "1.0",
  scalaVersion := "2.11.8",
  organization := "hr.fer.ztel",
  //mainClass in Compile := Some("hr.fer.ztel.dipl.run.add.MainAddInversJoinRdds"),
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  parallelExecution in Test := true
)

lazy val unprovidedDependencies = Seq(
   "com.github.fommil.netlib" % "all" % "1.1.2",

  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

libraryDependencies ++= unprovidedDependencies

val sparkVersion = "2.1.0.cloudera1"

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

val dev = "fpopic" // used on local machine
val user = sys.env.getOrElse("USER", dev)

if (user == dev)
  libraryDependencies ++= providedDependencies
else
  libraryDependencies ++= providedDependencies.map(_ % "provided")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

resolvers ++= Seq(
  "pentaho-releases" at "http://repository.pentaho.org/artifactory/repo/",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)
