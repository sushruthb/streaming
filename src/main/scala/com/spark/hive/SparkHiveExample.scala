package com.spark.hive

import java.io.File

import javax.script.ScriptException
import org.apache.log4j._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SparkHiveExample {
  case class Record(key: Int, value: String)

  def main(args:Array[String]): Unit = {
    //val warehouseLocation = new File("/apps/spark/warehouse").getAbsolutePath

    val warehouseLocation = new File( "/warehouse/tablespace/managed/hive" ).getAbsolutePath
    Logger.getLogger( "org" ).setLevel( Level.ERROR )
    val spark = SparkSession
      .builder()
      .appName( "Spark Hive Example" )
      .config( "spark.sql.warehouse.dir", warehouseLocation )
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("show databases")

    if(!spark.catalog.databaseExists("hive")){
      sql("create database hive")
    }

    if (spark.catalog.databaseExists( "hive" )) {
     // sql( "drop database hive cascade" )

      sql("use hive")
      sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) using hive")
      // sql("LOAD DATA INPATH '/user/hdfs/data/kv1.txt' INTO TABLE src")
      sql("load data inpath '/user/hdfs/data/kv1.txt' into table src")

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
      // You can also use DataFrames to create temporary views within a SparkSession.
      val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
      recordsDF.createOrReplaceTempView("records")

      // Queries can then join DataFrame data with data stored in Hive.
      sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

      // `USING hive`
        sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")

      // Save DataFrame to the Hive managed table
      val df = spark.table("src")

      df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
      // After insertion, the Hive managed table has data now
      sql("SELECT * FROM hive_records").show()
    }

    spark.close()
  }

}
