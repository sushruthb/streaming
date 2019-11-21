package com.struct.kafka

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession



object KafkaProduceJson {

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=ConfigFactory.load()
    val servers = conf.getString("prod.kafa.brokers")

    val spark= SparkSession
      .builder()
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq((1,"James ","","Smith",2018,1,"M",3000),
      (2,"Michael ","Rose","",2010,3,"M",4000),
      (3,"Robert ","","Williams",2010,3,"M",4000),
      (4,"Maria ","Anne","Jones",2005,5,"F",4000),
      (5,"Jen","Mary","Brown",2010,7,"",-1)
    )

    val columns = Seq("id","firstname","middlename","lastname","dob_year", "dob_month","gender","salary")
    import spark.implicits._
    val df = data.toDF(columns:_*)

    val ds = df.toJSON
    ds.printSchema()

    val query = ds.selectExpr( "CAST(value AS STRING), to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("topic", "text-topic")
      .start()

  }
}