package com.spark.ml

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
object PredictTitanic {

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").getLevel(Level.ERROR)

    val spark=SparkSession.builder()
      //.config( "spark.driver.bindAddress", "127.0.0.1" )
      .appName("TitanicPrediction").getOrCreate()

    val df=spark.read.csv("/user/hdfs/data/titanic.csv")
    df.show(4)

  }

}
