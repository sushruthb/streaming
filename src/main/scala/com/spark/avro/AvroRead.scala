package com.spark.avro

import org.apache.log4j._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.databricks.spark.avro._

object AvroRead {

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().enableHiveSupport().appName("AvroRead").getOrCreate()

    val df=spark.read.format("com.databricks.spark.avro").load("/user/hdfs/users.avro")
    df.show()
    df.write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save("/user/hdfs/avro/users.avro")


   /* import spark.sql

    sql("use database hive")
    sql("create external table users(name,favourite_color,favourite_numbers)")*/

  }

}
