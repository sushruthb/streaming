package com.struct.kafka

import java.util.{Collections, Properties}
import java.util.regex.Pattern

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters._
object KafkaConsumerSubscribeApp extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=ConfigFactory.load()
  val servers = conf.getString("prod.kafa.brokers")

  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers",servers)
  props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("txt_topic")
  try {
    consumer.subscribe(topics.asJava)
    //consumer.subscribe(Collections.singletonList("topic_partition"))
    //consumer.subscribe(Pattern.compile("topic_partition"))
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value() +
          ", Offset: " + record.offset() + ", Partition: " + record.partition())
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
}