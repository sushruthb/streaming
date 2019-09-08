package com.struct.kafka

import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object ReadFromKafka {



  def main(args:Array[String]): Unit ={
      val spark = SparkSession
      .builder
      .appName("Read-Kafka-Topic")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    //Subscribe the Stream From Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "struct_streaming")
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
      .option("truncate","false")
      .start()
      .awaitTermination()


  }





}
