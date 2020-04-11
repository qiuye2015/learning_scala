package com.dangdang.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class ConsumerClient(props: java.util.Properties, topic: String, partitions: Set[Int]
                     /*,dirName: String, offsetFilePrefix: String*/) {
  private var consumer: KafkaConsumer[String, String] = _
  private var topicPartitions: scala.collection.mutable.Set[TopicPartition] = scala.collection.mutable.Set()

  def initConsumer(): Unit = {
    try {
      consumer = new KafkaConsumer[String, String](props)
      setTopicPartitions()
      consumer.assign(topicPartitions)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  private def setTopicPartitions(): Unit = {
    val itPartition = partitions.iterator()
    while (itPartition.hasNext) {
      val topicPartition = new TopicPartition(topic, itPartition.next())
      topicPartitions += topicPartition
    }
  }

  def consumeData(timeoutMs: Long = 10L): Array[String] = {
    val records = consumer.poll(java.time.Duration.ofSeconds(timeoutMs))
    val a = ArrayBuffer[String]()

    if (records.count() > 0) {
      records.foreach(record => {
        //        val topicPartition = new TopicPartition(record.topic(), record.partition())
        //        consumedPartitionOffset += (topicPartition -> new OffsetAndMetadata(record.offset() + 1))
        a += record.value()
      })
      a.toArray
    } else {
      Array()
    }
  }
}
