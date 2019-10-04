lazy val root = (project in file(".")).
  settings(
name := "StructuredToKafka",
version := "0.1",
scalaVersion := "2.11.12"

)

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1",
  "org.apache.kafka" %% "kafka" % "2.0.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-hive" % "2.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-avro
  "org.apache.spark" %% "spark-avro" % "2.4.0"



//"com.typesafe" % "config" % "1.3.0" % "provided",
 // "za.co.absa" %% "abris" % "2.2.2" % "provided",
 // "com.lihaoyi" %% "ujson" % "0.6.6" % "compile"
)
resolvers += "confluent" at "http://packages.confluent.io/maven/"
resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/eed3si9n/sbt-plugins/"))(Resolver.ivyStylePatterns)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 //To add Kafka as source
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case x => MergeStrategy.first
}