
name := "GTFS Reasoner"
version := "0.1"
scalaVersion := "2.10.5"

jarName in assembly := "gtfs-reasoner-assembly.jar"
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


assemblyOption in assembly ~= { _.copy(includeScala = false) }

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.5.0" % "provided",
  "io.spray" %% "spray-json" % "1.3.2",
  ("org.apache.spark" %% "spark-streaming-kafka" % "1.5.0").
  exclude("commons-beanutils", "commons-beanutils").
  exclude("commons-collections", "commons-collections").
  exclude("commons-logging", "commons-logging").
  exclude("com.esotericsoftware.minlog", "minlog")
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case x if x.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.last
    case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
    case x if x.startsWith("plugin.properties") => MergeStrategy.last
    case x if x.endsWith("UnusedStubClass.class") => MergeStrategy.last
    case x => old(x)
  }
}

//libraryDependencies ++= Seq(
  //"org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
  //"org.apache.spark" %% "spark-streaming" % "1.5.0" % "provided",
  //"org.apache.spark" %% "spark-streaming-kafka" % "1.5.0",
  //"io.spray" %% "spray-json" % "1.3.2"
//)



