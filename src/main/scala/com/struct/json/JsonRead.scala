package com.struct.json

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
object JsonRead {

  def main(args:Array[String]): Unit ={

      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark=SparkSession.builder().master("local[*]").appName("JSonData").getOrCreate()

      import spark.implicits._

    val path = "src/main/resources/person.json"
    val peopleDF = spark.read.json(path)
    peopleDF.printSchema()

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("person")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = spark.sql("SELECT firstname FROM person WHERE dob_year BETWEEN 2010 AND 2018")
    teenagerNamesDF.show()
  }

}
