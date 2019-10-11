package com.struct.avro

import com.typesafe.config.ConfigFactory
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.avro.SchemaBuilder
import org.apache.spark.sql.avro._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{get_json_object, json_tuple}
object AvroSink {
    def main(args:Array[String]): Unit ={
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf=ConfigFactory.load()


      val spark= SparkSession
        .builder()
        .appName("SparkByExample.com")
        .getOrCreate()

      val topic = "avro_topic"
      val servers = conf.getString("prod.kafa.brokers")



      import spark.implicits._

      val df = spark
   /*     .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", "text_topic")
        .load()
        .select(
          from_avro($"key", SchemaBuilder.builder().stringType() ).as("key"),
          from_avro($"value", SchemaBuilder.builder().intType()).as("value"))*/

      // Convert structured data to binary from string (key column) and
      // int (value column) and save them to a Kafka topic.
     /* df
        .select(
          to_avro($"key").as("key"),
          to_avro($"value").as("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("topic", topic)
       // .save()*/
    }


}
