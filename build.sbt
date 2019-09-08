lazy val root = (project in file(".")).
  settings(
name := "StructuredToKafka",
version := "0.1",
scalaVersion := "2.11.12"

)

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1",
  "org.apache.kafka" %% "kafka" % "2.0.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.3.2",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "org.apache.spark" %% "spark-hive" % "2.3.0",
  "mysql" % "mysql-connector-java" % "5.1.6" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2",
  "com.twitter" %% "bijection-avro" % "0.9.2",

  "com.typesafe" % "config" % "1.3.0" % "provided",
  "za.co.absa" %% "abris" % "2.2.2" % "provided",
  "com.lihaoyi" %% "ujson" % "0.6.6" % "compile"
)
resolvers += "confluent" at "http://packages.confluent.io/maven/"
resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/eed3si9n/sbt-plugins/"))(Resolver.ivyStylePatterns)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}