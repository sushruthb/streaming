package com.struct.kafka

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics>")
      System.exit(1)
    }

    val Array(bootstrapServers, subscribeType, topics) = args

    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      .getOrCreate()

    import spark.implicits._
    val conf=ConfigFactory.load()
    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("prod.kafa.brokers"))
      .option(subscribeType, conf.getString("topic"))
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}