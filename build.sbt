lazy val root = (project in file(".")).
  settings(
name := "StructuredToKafka",
version := "0.1",
scalaVersion := "2.11.12"

)

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.kafka" % "kafka-clients" % "2.4.0",
  "org.apache.kafka" %% "kafka" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.3.2",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "org.apache.spark" %% "spark-hive" % "2.3.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2",
  // https://mvnrepository.com/artifact/com.databricks/spark-avro
  "com.databricks" %% "spark-avro" % "4.0.0",
  "com.typesafe" % "config" % "1.3.2",
  "com.typesafe.akka" %% "akka-actor" % "2.5.14",
  "org.apache.hive" % "hive-jdbc" % "3.1.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.5.1",
  "org.ehcache" % "ehcache" % "3.3.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.12.0"




)

unmanagedResourceDirectories in Compile += { baseDirectory.value / "src/main/resources"}

resolvers += "confluent" at "http://packages.confluent.io/maven/"
resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "MavenCentral" at "https://mvnrepository.com/"
resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/eed3si9n/sbt-plugins/"))(Resolver.ivyStylePatterns)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 //To add Kafka as source
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case x => MergeStrategy.first
}