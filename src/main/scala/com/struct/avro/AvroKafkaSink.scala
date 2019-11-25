package com.struct.avro

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.avro.Schema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AvroKafkaSink {

   def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = ConfigFactory.load()

      val schema = new Schema.Parser().parse(new File("/home/dev/streaming/src/main/resources/avro/ATTACH_CONTENT_AF_A.avsc"))
      val spark = SparkSession.builder.appName("AvroFormat").getOrCreate()

     val df = spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", conf.getString("prod.kafa.brokers"))
       .option("avroSchema", schema.toString)
       .option("subscribe", "topic").load()
      //val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "wdfl41000200d.emea.global.corp.sap:6667,wdfl41000341d.emea.global.corp.sap:6667,wdfl41000178d.emea.global.corp.sap:6667").option("subscribe", "topic").load()
     df.printSchema()

     //val usersDF = spark.read.format("avro").option("avroSchema", schema.toString).load("/home/dev/streaming/src/main/resources/users.avro")
    // usersDF.select("name", "favorite_color").write.format("avro").save("/home/dev/streaming/src/main/resources/namesAndFavColors.avro")

      import spark.implicits._


    }
  }
