package com.spark.streaming.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
object ReadCSVStreaming {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName( "ReadCSVStreaming" )
      .getOrCreate()

    import spark.implicits._


    val userSchema = new StructType().add("Code", "integer").add("Description", "string").add("SortOrder","integer")
    val csvDF = spark
      .read
      .option("sep", ",")
      .schema(userSchema)   // Specify schema of the csv files
      .format("csv")
      .load("hdfs:///data/DimenLookupAge8317.csv")
     // .csv("hdfs:///data/DimenLookupAge8317.csv") // Equivalent to format("csv").load("/path/to/directory")


    val words = csvDF.as[String].flatMap(_.split(","))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("update")
      .format("console")
      .start()
    query.awaitTermination()
  }

}
