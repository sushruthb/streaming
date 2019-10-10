package com.struct.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types._

object ReadFromKafka {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession
      .builder
      .appName("Read-Kafka-Topic")
      .getOrCreate()

    import spark.implicits._

    //Subscribe the Stream From Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667")
      .option("subscribe", "str_stre")
      .option("startingOffsets","earliest")
      .load()



    val mySchema = StructType(Array(
      StructField("HSCode", IntegerType),
      StructField("Commodity", StringType),
      StructField("value", DoubleType),
      StructField("country", StringType),
      StructField("year", IntegerType)
    ))

    import org.apache.spark.sql.functions.from_json

    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", mySchema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    //Print the DataFrame on Console


    df1.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate","false")
      .start()
      .awaitTermination()


  }





}
