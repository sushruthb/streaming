package com.spark.ml
import org.apache.log4j._
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession
object BinarizerExample {

  def main(args:Array[String]): Unit ={

      Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder.appName("BinarizerExample").master("local[*]").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()

    val data=Array((0,0.1),(1,0.8),(2,0.2))
    val dataframe=spark.createDataFrame(data).toDF("id","feature")

    val binarizer: Binarizer=new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binariedDataFrame=binarizer.transform(dataframe)

    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binariedDataFrame.show()

    spark.stop()





  }

}
