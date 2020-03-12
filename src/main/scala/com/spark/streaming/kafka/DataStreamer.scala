package com.spark.streaming.kafka

import java.util.Properties

import akka.actor.ActorSystem
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.spark.streaming.ConfigReader._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random
import com.spark.streaming.LoggerHelper
import org.apache.spark.sql.SparkSession

object DataStreamer extends LoggerHelper {
  private val properties = new Properties ()
  def main(args:Array[String]) {
    val spark=SparkSession.builder().appName("SparkProducer").getOrCreate()
    val lines = spark.readStream
      .format("socket")
      .option("bootstrap.servers", "bootstrapServer")
      .option("key.serializer", "serializer")
      .option("value.serializer", "serializer")
      .load()
  lazy val system = ActorSystem ("data-streamer")

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher


  properties.put ("bootstrap.servers", bootstrapServer )
  properties.put ("key.serializer", serializer)
  properties.put ("value.serializer", serializer)

  val producer = new KafkaProducer[String, String] (properties)

  val randomWords = List ("confluent", "databricks", "lightbend", "datastax", "akka")

  info ("Streaming words to kafka..")
  system.scheduler.schedule (0 seconds, 1 seconds) {
  Random.shuffle (randomWords).foreach {
  word =>
  producer.send (new ProducerRecord[String, String] ("topic2", word) )
}
}
}
}
