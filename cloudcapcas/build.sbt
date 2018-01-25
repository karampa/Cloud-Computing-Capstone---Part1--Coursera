name := "cloudcapcas"

version := "1.0"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// https://groups.google.com/a/lists.datastax.com/forum/#!topic/spark-connector-user/5muNwRaCJnU
assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
}

scalaVersion := "2.11.8"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
// use provided line when building assembly jar
// "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
// comment above line and uncomment the following to run in sbt
   "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
   "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2"
)
