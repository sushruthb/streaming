package com.spark.ml

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.ml.feature.StringIndexer
object PredictTitanic {

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder()
      //.config( "spark.driver.bindAddress", "127.0.0.1" )
      .appName("TitanicPrediction").getOrCreate()

    val df=spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("/user/hdfs/data/titanic.csv")

    val inputs=df.drop("PassengerId","Name","SibSp","Parch","Ticket","Embarked","Cabin")
    val target=df.select("Survived")

    val features=inputs
    val indexer=new StringIndexer()
      .setInputCol("Sex")
      .setOutputCol("Sex_n")
      .setInputCol("Age")
      .setOutputCol("Age_n")
      .setInputCol("Fare")
      .setOutputCol("Fare_n")

    val indexed=indexer.fit(df).transform(df)
    indexed.show(5)

  }

}
