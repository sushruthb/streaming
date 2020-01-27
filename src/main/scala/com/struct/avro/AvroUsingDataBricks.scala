package com.struct.avro

import java.io.File

import com.databricks.spark.avro._
import org.apache.avro.Schema
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Spark Avro library example
 * Avro schema example
 * Avro file format
 *
 */
object AvroUsingDataBricks {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkAVRO.com")
      .getOrCreate()

    val data = Seq(("James ","","Smith",2018,1,"M",3000),
      ("Michael ","Rose","",2010,3,"M",4000),
      ("Robert ","","Williams",2010,3,"M",4000),
      ("Maria ","Anne","Jones",2005,5,"F",4000),
      ("Jen","Mary","Brown",2010,7,"",-1)
    )

    val columns = Seq("firstname","middlename","lastname","dob_year",
      "dob_month","gender","salary")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)

    /**
     * Write Avro File
     */
    df.write
      .mode(SaveMode.Overwrite)
      .avro("/user/hdfs/data/avro/person.avro")

    //Alternatively you can specify the format to use instead:
    df.write.format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save("/user/hdfs/data/avro/person2.avro")

    /**
     * Read Avro File
     */
    val readDF = spark.read.avro("/user/hdfs/data/avro/person.avro")
    //Alternatively you can specify the format to use instead:

    val readDF2 = spark.read
      .format("com.databricks.spark.avro")
      .load("/user/hdfs/data/avro/person2.avro")

    /**
     * Write Partition
     */
    df.write.partitionBy("dob_year","dob_month")
      .mode(SaveMode.Overwrite)
      .avro("/user/hdfs/data/avro/person_partition.avro")

    /**
     * Reading Partition Data
     */
    spark.read
      .avro("/user/hdfs/data/avro/person_partition.avro")
      .where(col("dob_year") === 2010)
      .show()

    /**
     * Namespace
     */
    val name = "AvroTest"
    val namespace = "com.sparkbyexamples.spark"
    val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)
    df.write.options(parameters)
      .mode(SaveMode.Overwrite)
      .avro("/user/hdfs/data/avro/person_namespace.avro")

    /**
     * Explicit schema
     */
    val schemaAvro = new Schema.Parser()
      .parse(new File("/user/hdfs/data/avro/person.avsc"))

    spark
      .read
      .format("com.databricks.spark.avro")
      .option("avroSchema", schemaAvro.toString)
      .load("/user/hdfs/data/avro/person.avro")
      .show()
    import spark.sql
    /**
     * Spark SQL
     */
 //   sql("CREATE TEMPORARY VIEW PERSON USING com.databricks.spark.avro OPTIONS (path \"/user/hdfs/data/avro/person.avro\")")
   // val df2 = sql("SELECT * FROM PERSON").show()
  }
}