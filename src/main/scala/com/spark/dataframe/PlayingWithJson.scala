package com.spark.dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.spark.dataframe.Constants._
import com.spark.streaming.LoggerHelper
object PlayingWithJson extends App with LoggerHelper{

  //creating a json
  val jsonDf: DataFrame =
    spark.range(1)
    .selectExpr("""'{"framework": "spark", "version" : "2.3", "company": "databricks" }' as jsonString""")

  jsonDf.show(false)

  //use get_json_object to inline query a JSON object
  jsonDf.select(get_json_object(col("jsonString"), "$.framework")).show(false)

}
