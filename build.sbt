name := "spark-recommender"
version := "1.0"
scalaVersion := "2.11.8"

lazy val unprovidedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-mllib" % "2.1.1"
)

libraryDependencies ++= unprovidedDependencies