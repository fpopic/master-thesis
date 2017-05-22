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
  "com.github.fommil.netlib" % "all" % "1.1.2"
)

libraryDependencies ++= unprovidedDependencies

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-mllib" % "2.1.1"
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