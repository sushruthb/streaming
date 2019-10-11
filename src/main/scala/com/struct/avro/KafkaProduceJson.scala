package com.struct.avro
import com.typesafe.config.ConfigFactory
import org.apache.commons.configuration.ConfigurationFactory
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object KafkaProduceJson {
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=ConfigFactory.load().getConfig(args(0))


    val spark = SparkSession
      .builder()
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data1 = Seq((1,"James ","","Smith",2018,1,"M",3000),
      (2,"Michael ","Rose","",2010,3,"M",4000),
      (3,"Robert ","","Williams",2010,3,"M",4000),
      (4,"Maria ","Anne","Jones",2005,5,"F",4000),
      (5,"Jen","Mary","Brown",2010,7,"",-1)
    )

    val columns = Seq("id","firstname","middlename","lastname","dob_year",
      "dob_month","gender","salary")
    import spark.implicits._


    val df = data1.toDF(columns:_*)

   // val ds = df.toDF().toJSON

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



  //  val df1 = df.selectExpr("CAST(firstname AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
   //   .select(from_json($"value", mySchema).as("data"), $"timestamp")
  //    .select("data.*", "timestamp")


    df.printSchema()

    val query = df.selectExpr("CAST(firstname AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option( "kafka.bootstrap.servers", conf.getString("prod.kafa.brokers") )
      .option("topic", "text_topic")
      .save()


  }

}
