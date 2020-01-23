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
    sql("show databases").show()
    //sql("create database hivetest")
    //sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/user/hdfs/data/DimenLookupAge8317.csv")

    peopleDFCsv.show(10)

    if(spark.catalog.databaseExists("hive")) {
      sql( "use hive" )

      if (!spark.catalog.tableExists( "DimenLookupAge" )) {
        sql( " create table if not exists DimenLookupAge(code Int,description String, sortOrder INT) row format delimited fields terminated by \",\" lines terminated by \"\\n\"" )
      }

      sql( "load data inpath '/user/hdfs/data/' into table DimenLookupAge" )


     // peopleDFCsv.toDF().write.saveAsTable( "DimenLookupAge" )

    }
  }
}
