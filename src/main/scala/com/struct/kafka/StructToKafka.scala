package com.struct.kafka

import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object StructToKafka {
  def main(args:Array[String]){
  Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=ConfigFactory.load()
  val spark = SparkSession
    .builder
    .appName("Spark-Kafka-Integration")
    .getOrCreate()

import spark.implicits._
      //Define the Schema

  val mySchema = StructType(Array(
    StructField("HSCode", IntegerType),
    StructField("Commodity", StringType),
    StructField("value", DoubleType),
    StructField("country", StringType),
    StructField("year", IntegerType)
  ))
  //Create the Streaming DataFrame
  val streamingDataFrame = spark.readStream.schema(mySchema).csv("/user/hdfs/data/kafka")

    streamingDataFrame.printSchema()
    //Publish the Stream to Kafka

  val query=streamingDataFrame.selectExpr("CAST(HSCode AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .outputMode("append")
    .option( "kafka.bootstrap.servers", conf.getString("prod.kafa.brokers"))
    .option("topic", "str-str")
    .option("checkpointLocation", "/home/hdfs/checkpoint"+UUID.randomUUID.toString)
    .start()
    .awaitTermination()

   // spark.stop

  }

}
