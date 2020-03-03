package com.spark.avro

import org.apache.log4j._
import org.apache.spark.sql.{SaveMode, SparkSession}

object AvroRead {

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().enableHiveSupport().appName("AvroRead").getOrCreate()

    val df=spark.read.format("com.databricks.spark.avro").load("src/main/resources/users.avro")
    df.show()
    df.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save("/user/hdfs/avro/users.avro")


   /* import spark.sql

    sql("use database hive")
    sql("create external table users(name,favourite_color,favourite_numbers)")*/

  }

}
