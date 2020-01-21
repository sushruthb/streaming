package com.spark.hive

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SparkHiveExample {
  case class Record(key: Int, value: String)

  def main(args:Array[String]): Unit ={
    val warehouseLocation = new File("/apps/spark/warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    //sql("create database hive")
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) using hive")
   // sql("LOAD DATA INPATH '/user/hdfs/data/kv1.txt' INTO TABLE src")
    sql("SELECT * FROM src").show()
    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

    // `USING hive`
    sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
    // Save DataFrame to the Hive managed table
    val df = spark.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM hive_records").show()



    spark.close()
  }

}
