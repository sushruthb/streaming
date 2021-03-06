package com.struct.kafka

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadSaveToHDFS {

 def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=ConfigFactory.load()
      val spark = SparkSession
      .builder
      .appName("Read-Kafka-Topic")
      .getOrCreate()

    import spark.implicits._

    //Subscribe the Stream From Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("prod.kafa.brokers"))
      .option("subscribe", "str-str")
      .option("startingOffsets","earliest")
      .option("failOnDataLoss", "true")
      .load()



    val mySchema = StructType(Array(
      StructField("HSCode", IntegerType),
      StructField("Commodity", StringType),
      StructField("value", DoubleType),
      StructField("country", StringType),
      StructField("year", IntegerType)
    ))
  df.printSchema()
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

   val result = df1.toDF("value","timestamp")

   result.coalesce(3)
     .write
     .format("com.databricks.spark.avro")
     .mode(SaveMode.Append)
     .save("/user/hdfs/data/avro")




  }





}
