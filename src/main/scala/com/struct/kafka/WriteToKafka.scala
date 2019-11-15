package com.struct.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types._
import com.struct.kafka._
import com.typesafe.config.ConfigFactory

object WriteToKafka {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=ConfigFactory.load()
      val spark = SparkSession
      .builder
      .appName("Write-To-Kafka")
      .getOrCreate()

    import spark.implicits._

    //Subscribe the Stream From Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option( "kafka.bootstrap.servers", conf.getString("prod.kafa.brokers") )
      .option("subscribe", "str_str")
      .load()



    val mySchema = StructType(Array(
      StructField("HSCode", IntegerType),
      StructField("Commodity", StringType),
      StructField("value", DoubleType),
      StructField("country", StringType),
      StructField("year", IntegerType)
    ))

    import org.apache.spark.sql.functions.from_json

    val df1 = df.selectExpr("CAST(value AS STRING) AS key", "to_json(struct(*)) AS value").as[(String, String)]


    //Write Dataframe to Kafka

    df1.writeStream
      .format("kafka")
      .option("topic", "str_stre")
      .outputMode("append")
      .option("kafka.bootstrap.servers",conf.getString("prod.kafa.brokers"))
      .option("checkpointLocation", "/home/hdfs/checkpoint5")
      .start()
      .awaitTermination()

      spark.stop()
  }
}
