package com.spark.ml

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
object TitanicPrediction {

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
      val spark=SparkSession.builder().appName("ML").getOrCreate()

   // val df =spark.read.csv("src/main/resources/ml/titanic.csv")
   val df =spark
     .read
       .option("header","true")
     .option("inferSchema","true")
     .csv("/user/hdfs/data/titanic.csv")


    df.printSchema()
    df.head()

    df.drop("PassengerId","Name","SibSp","Parch","Ticket","Cabin","Embarked")
    df.head()

   // val inputs=df.drop("Survived")
   // val target=inputs.select("Survived")

    //target.show()







  }

}
