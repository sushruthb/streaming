 package com.struct.avro
import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.avro.Schema
import java.nio.file.Files
import java.nio.file.Paths;
object KafkaConsumeAvro {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = ConfigFactory.load()
    val spark = SparkSession
      .builder()
      .appName("SparkByExample.com")
      .getOrCreate()

      // `from_avro` requires Avro schema in JSON string format.
    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("./src/main/resources/avro/ATTACH_CONTENT_AF_A.avsc")))
    val schema = new Schema.Parser().parse(new File("user.avsc"))
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("prod.kafa.brokers"))
      .option("subscribe", "DC18PRODUCTION_CAL_ATTACH_CONTENT_AF_A")
      .load()

    // 1. Decode the Avro data into a struct;
    // 2. Filter by column `favorite_color`;
    // 3. Encode the column `name` in Avro format.

    import spark.implicits._

    val output = df
      .select(from_avro('value, jsonFormatSchema) as 'user)
      .where("user.favorite_color == \"red\"")
      .select(to_avro($"user.name") as 'value)

  }
}