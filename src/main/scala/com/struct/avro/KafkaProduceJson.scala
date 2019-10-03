package com.struct.avro
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object KafkaProduceJson {
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder()
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq((1,"James ","","Smith",2018,1,"M",3000),
      (2,"Michael ","Rose","",2010,3,"M",4000),
      (3,"Robert ","","Williams",2010,3,"M",4000),
      (4,"Maria ","Anne","Jones",2005,5,"F",4000),
      (5,"Jen","Mary","Brown",2010,7,"",-1)
    )

    val columns = Seq("id","firstname","middlename","lastname","dob_year",
      "dob_month","gender","salary")
    import spark.implicits._
    val df = data.toDF(columns:_*)

    val ds = df.toJSON

    val mySchema = StructType(Array(
      StructField("firstname", StringType),
      StructField("middlename", StringType),
      StructField("lastname", StringType),
      StructField("dob_year", IntegerType),
      StructField("dob_month", IntegerType),
      StructField("gender", StringType),
      StructField("salary", IntegerType)
    ))

    import org.apache.spark.sql.functions.from_json

    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", mySchema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    df1.printSchema()

    val query = df1
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.76.106.229:6667,10.76.107.133:6667,10.76.117.167:6667")
      .option("topic", "text_topic")
      .start()

  }

}
