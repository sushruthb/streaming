package com.spark.dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.spark.dataframe.Constants._
import com.spark.streaming.LoggerHelper
object DataFrameExamples extends App with LoggerHelper {

  //create sparksession
  val spark: SparkSession = SparkSession.builder().appName("dataframe-examples").getOrCreate()

  //read json file as dataframe
  val df: DataFrame = spark.read.json("file:///2015-summary.json")

  //shows only first 20 rows
  df.show()

  //selecing specific column
  df.select(col("ORIGIN_COUNTRY_NAME"), column("DEST_COUNTRY_NAME")).show(4)

  df.selectExpr("DEST_COUNTRY_NAME").show(4)

  //adding new column
  df.withColumn("literal", lit(1)).show(4)

  //renaming a column
  df.withColumn("literal", lit(1))
    .withColumnRenamed("literal", "lit").show(4)

  //dropping a column
  df.drop(col("DEST_COUNTRY_NAME")).show(4)

  //filtering a dataframe
  df.filter(col("count") > 23).show(4)

  df.createOrReplaceTempView("dftable")
  df.sqlContext.sql("select * from dftable;").show(4)
}
