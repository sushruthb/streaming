package com.struct.avro

import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark
import org.apache.spark.sql.avro._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{get_json_object, json_tuple}
object AvroSink {
    def main(args:Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = ConfigFactory.load()

      val schema = new Schema.Parser().parse(new File("/home/dev/streaming/src/main/resources/user.avsc"))
      val spark=SparkSession.builder.appName("AvroFormat").getOrCreate()

      val usersDF = spark.read.format("avro").option("avroSchema", schema.toString).load("/home/dev/streaming/src/main/resources/users.avro")
      usersDF.select("name", "favorite_color").write.format("avro").save("/home/dev/streaming/src/main/resources/namesAndFavColors.avro")

      import spark.implicits._




    }

}
