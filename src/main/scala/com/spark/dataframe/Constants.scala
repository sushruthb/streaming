package com.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

object Constants {

  val spark: SparkSession = SparkSession.builder().appName("dataframe-operations").getOrCreate()
val resource=getClass.getResourceAsStream("/retailer.csv").toString
  val df: DataFrame = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(resource)
//  getClass.getResourceAsStream("/data/url_list1.csv")
}
