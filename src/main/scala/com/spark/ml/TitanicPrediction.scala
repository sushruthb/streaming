package com.spark.ml

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
object TitanicPrediction {

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
      val spark=SparkSession.builder().appName("ML").master("local[*]").getOrCreate()

   // val df =spark.read.csv("src/main/resources/ml/titanic.csv")
   val df =spark.read.csv("/user/titanic.csv")
    df.printSchema()




  }

}
