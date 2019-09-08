package com.struct.kafka

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object StructToKafka {
  def main(args:Array[String]){
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder
    .appName("Spark-Kafka-Integration")
    .master("local")
    .getOrCreate()

  //Define the Schema

  val mySchema = StructType(Array(
    StructField("HSCode", IntegerType),
    StructField("Commodity", StringType),
    StructField("value", DoubleType),
    StructField("country", StringType),
    StructField("year", IntegerType)
  ))

  //Create the Streaming DataFrame
  val streamingDataFrame = spark.readStream.schema(mySchema).csv("./src/main/resources/2018-2010_export.csv")

    //Publish the Stream to Kafka

  streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "struct_streaming")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "path to your local dir")
    .start()
  }

}
