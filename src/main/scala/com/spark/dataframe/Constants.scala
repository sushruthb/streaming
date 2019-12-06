package com.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

object Constants {

  val spark: SparkSession = SparkSession.builder().appName("dataframe-operations").getOrCreate()

  val df: DataFrame = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("file:///src/main/resources/retailer.csv")

}
