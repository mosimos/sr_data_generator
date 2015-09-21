
name := "GTFS Reasoner"
version := "0.1"
scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.5.0" % "provided",
  //"org.apache.spark" %% "spark-streaming-kafka" % "1.5.0",
  "io.spray" %% "spray-json" % "1.3.2"
)


jarName in assembly := "gtfs-reasoner-assembly.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


