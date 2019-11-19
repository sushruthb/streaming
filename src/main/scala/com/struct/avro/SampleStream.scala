package com.struct.avro

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.parquet.Files
import org.apache.twill.internal.utils.Paths

object SampleStream {

  import org.apache.spark.sql.functions.{get_json_object, json_tuple}

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=ConfigFactory.load()
    val spark = SparkSession
      .builder()
      .appName( "SparkByExample.com" )
      .getOrCreate()

    import org.apache.spark.sql.avro._
    import java.nio.file.Files;
    import java.nio.file.Paths;
    // `from_avro` requires Avro schema in JSON string format.
    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("./src/main/resources/user.avsc")))

    var df = spark.readStream
        .format( "kafka" )
        .option( "kafka.bootstrap.servers", conf.getString("prod.kafa.brokers") )
        .option("subscribe", "topic1")
        .load()

    df.printSchema()

    import spark.implicits._

    val output = df
      .select(from_avro('value, jsonFormatSchema) as 'user)
      .where("user.favorite_color == \"red\"")
      .select(to_avro($"user.name") as 'value)

    val query = output
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("prod.kafa.brokers"))
      .option("topic", "topic2")
      .start()
  }
}