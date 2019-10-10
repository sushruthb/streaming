lazy val root = (project in file(".")).
  settings(
name := "StructuredToKafka",
version := "0.1",
scalaVersion := "2.11.12"

)

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.kafka" % "kafka-clients" % "2.3.0",
  "org.apache.kafka" %% "kafka" % "2.3.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10_2.11" % "2.4.0",
  "org.apache.spark" %% "spark-sql_2.11" % "2.4.0",
  "org.apache.spark" %% "spark-hive" % "2.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
  "org.apache.spark" %% "spark-avro_2.11" % "2.4.0"
)
resolvers += "confluent" at "http://packages.confluent.io/maven/"
resolvers += "MavenCentral" at "https://mvnrepository.com/"
resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/eed3si9n/sbt-plugins/"))(Resolver.ivyStylePatterns)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 //To add Kafka as source
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case x => MergeStrategy.first
}