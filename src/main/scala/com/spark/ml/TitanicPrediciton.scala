package com.spark.ml

import com.spark.streaming.LoggerHelper
import org.apache.spark.sql.SparkSession
object TitanicPrediciton extends LoggerHelper {

  def main(args:Array[String]): Unit ={
      val spark=SparkSession.builder().appName("ML").getOrCreate()

    val df =spark.read.csv("/user/titanic.csv")
    df.printSchema()


  }

}
