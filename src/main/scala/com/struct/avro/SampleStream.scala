package com.struct.avro

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object SampleStream {

  import org.apache.spark.sql.functions.{get_json_object, json_tuple}

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName( "SparkByExample.com" )
      .getOrCreate()
    var streamingInputDF =
      spark.readStream
        .format( "kafka" )
        .option( "kafka.bootstrap.servers", "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667" )
        .option( "subscribe", "text_topic" )
        .option( "startingOffsets", "earliest" )
        .option( "minPartitions", "10" )
        .option( "failOnDataLoss", "true" )
        .load()

    streamingInputDF.printSchema()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    var streamingSelectDF =
      streamingInputDF
        .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"))
        .groupBy($"zip")
        .count()
    streamingSelectDF.printSchema()


    var streamingSelectDF1 =
      streamingInputDF
        .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"), get_json_object(($"value").cast("string"), "$.hittime").alias("hittime"))
        .groupBy($"zip", window($"hittime".cast("timestamp"), "10 minute", "5 minute", "2 minute"))
        .count()



    import org.apache.spark.sql.streaming.ProcessingTime

    val query =
      streamingSelectDF
        .writeStream
        .format("console")
        .outputMode("complete")
        .trigger(ProcessingTime("25 seconds"))
        .start()
      .awaitTermination()
  }
}