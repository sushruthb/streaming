package com.struct.avro

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object KafkaConsumer {

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = ConfigFactory.load()
    val spark = SparkSession
      .builder()
      .appName("SparkByExample.com")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("prod.kafa.brokers"))
      .option("subscribe", conf.getString("topic"))
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    df.show()

    spark.close()

  }

}
