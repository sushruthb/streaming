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
  val streamingDataFrame = spark.readStream.schema(mySchema).csv("/user/hdfs/")

    //Publish the Stream to Kafka

  val query=streamingDataFrame.selectExpr("CAST(HSCode AS STRING) AS key", "to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "str_stre")
    .option("kafka.bootstrap.servers", "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667")
    .outputMode("update")
    .option("checkpointLocation", "/home/hdfs/checkpoint")
    .start()

    query.awaitTermination()

    spark.stop

  }

}
