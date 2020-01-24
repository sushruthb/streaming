package com.spark.hive

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkHiveDF {

  def main(args:Array[String]): Unit = {
    val warehouseLocation = new File( "/warehouse/tablespace/managed/hive" ).getAbsolutePath
    Logger.getLogger( "org" ).setLevel( Level.ERROR )

    val conf=ConfigFactory.load()

    val spark = SparkSession
      .builder()
      .appName( "Spark Hive DataFrame Example" )
      .config( "spark.sql.warehouse.dir", warehouseLocation )
      .enableHiveSupport()
      .getOrCreate()

  val peopleDFCsv = spark.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(conf.getString("csv1.data"))

  peopleDFCsv.show(10)

    import spark.sql

    if(spark.catalog.databaseExists("hive")) {

      sql( "use hive" )

      if (!spark.catalog.tableExists( "DimenLookupAge1" )) {
        //sql( conf.getString( "hive2.query" ) )
        peopleDFCsv.toDF().write.saveAsTable( "DimenLookupAge1" )
      }
    }


    spark.close()

}
}

