package com.spark.avro

import java.io.File

import org.apache.log4j._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.databricks.spark.avro._
import org.apache.avro.Schema

object AvroSchema {

  def main(args:Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().config( "spark.driver.bindAddress", "127.0.0.1" ).master("local[*]").appName("AvroSave").getOrCreate()
    import spark.implicits._

    val df=spark.read.avro("src/main/resources/avro/userdata1.avro")

    df.show(4)

    //df.write.format("com.databricks.spark.avro").save("src/main/resources/avro/userdata2.avro")

/*    val schemaAvro = new Schema.Parser()
      .parse(new File("src/main/resources/avro/userdata.avsc"))
    spark
      .read
      .format("com.databricks.spark.avro")
      .option("avroSchema", schemaAvro.toString)
      .load("src/main/resources/avro/userdata1.avro")
      .show()*/


  }

}
