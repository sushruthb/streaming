package com.spark.avro

import java.io.File

import com.databricks.spark.avro._
import org.apache.avro.Schema
import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j._

object AvroWrite {

  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder()
      .config( "spark.driver.bindAddress", "127.0.0.1" )
      .master("local[*]")
      .appName("Avrodata")
      .getOrCreate()

    /**
     * Explicit schema
     */
    val schemaAvro = new Schema.Parser()
      .parse(new File("src/main/resources/user.avsc"))

    val df=spark
      .read
      .format("com.databricks.spark.avro")
      .option("avroSchema", schemaAvro.toString)
      .load("src/main/resources/users.avro")
      .show()
    import spark.implicits._
    /**
     * Spark SQL
     */
    spark.sql("CREATE TEMPORARY VIEW PERSON USING com.databricks.spark.avro OPTIONS (path \"./src/main/resources/users.avro\")")

    spark.sql("SELECT * FROM PERSON").show()




  }

}
