package com.struct.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types._
import com.struct.kafka.KafkaSink

object WriteToKafka {



  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession
      .builder
      .appName("Write-To-Kafka")
      .getOrCreate()

    import spark.implicits._

    //Subscribe the Stream From Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667")
      .option("subscribe", "str_stre")
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

    //Write Dataframe to Kafka

   /* df1.writeStream
      .format("kafka")
      .option("topic", "Struct_Streaming")
      .outputMode("update")
      .option("kafka.bootstrap.servers", "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667")
      .option("checkpointLocation", "/home/hdfs/checkpoint")
      .start()*/

    val topic = "str_stre"
    val brokers = "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667"

    val writer = new KafkaSink(topic, brokers)

    val query = df1
      .writeStream
      .foreach(writer)
      .outputMode("update")
      .start()

/*    val ds = df1
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667")
      .option("topic", "Struct_Streaming")
      .start()
      .awaitTermination()*/

      spark.stop()
  }
}
