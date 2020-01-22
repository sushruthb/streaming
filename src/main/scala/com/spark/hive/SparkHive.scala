package com.spark.hive

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args:Array[String]): Unit = {
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
    spark.sqlContext.sql("show databases")

  }
}
