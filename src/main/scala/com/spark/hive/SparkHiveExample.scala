package com.spark.hive

import java.io.File

import org.apache.spark.sql.SparkSession

object SparkHiveExample {
  case class Record(key: Int, value: String)

  def main(args:Array[String]): Unit ={
    val warehouseLocation = new File("hdfs:///warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    sql("use hive")
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
//    sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    spark.close()
  }

}
