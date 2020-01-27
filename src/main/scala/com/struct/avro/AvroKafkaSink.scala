package com.struct.avro

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, from_json}

object AvroKafkaSink {

   def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = ConfigFactory.load()

      val schema = new Schema.Parser().parse(new File("/home/dev/streaming/src/main/resources/avro/ATTACH_CONTENT_AF_A.avsc"))
      val spark = SparkSession.builder.appName("AvroFormat").getOrCreate()
     import spark.implicits._
     val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", conf.getString("prod.kafa.brokers")).option("startingOffsets", "earliest").option("avroSchema", schema.toString).option("subscribe", "topic").load().selectExpr("CAST(key AS STRING) AS key", "to_avro(struct(*)) AS value")


     //val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "wdfl41000200d.emea.global.corp.sap:6667,wdfl41000341d.emea.global.corp.sap:6667,wdfl41000178d.emea.global.corp.sap:6667").option("subscribe", "topic").load()
     df.printSchema()








    }
  }
