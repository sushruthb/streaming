package com.struct.avro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.functions.{col, from_json, struct}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.log4j._

object KafkaProduceAvro {

  def main(args: Array[String]): Unit = {

      val spark= SparkSession
      .builder()
      .appName("SparkByExample.com")
      .getOrCreate()

    /*
    Disable logging as it writes too much log
     */
    Logger.getLogger("org").setLevel(Level.ERROR)

    /*
    This consumes JSON data from Kafka
     */
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667")
      .option("subscribe", "text_topic")
      .option("startingOffsets", "earliest") // From starting
      .load()

    /*
     Prints Kafka schema with columns (topic, offset, partition e.t.c)
      */
    df.printSchema()

    val schema = new StructType()
      .add("id",IntegerType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",IntegerType)
      .add("dob_month",IntegerType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    /*
    Converts JSON string to DataFrame
     */
    val personDF = df.selectExpr("CAST(value AS STRING)") // First convert binary to string
      .select(from_json(col("value"), schema).as("data"))


    personDF.printSchema()


    /*
      * Convert DataFrame columns to Avro format and name it as "value"
      * And send this Avro data to Kafka topic
      */

    personDF.select(to_avro(struct("data.*")) as "value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667")
      .option("topic", "avro_topic")
      .option("checkpointLocation","/home/hdfs/checkpoint")
      .start()
      .awaitTermination()
  }


}
