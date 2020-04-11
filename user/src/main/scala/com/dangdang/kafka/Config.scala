package com.dangdang.kafka

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig

class consumerConfig {
  val props: Properties = new Properties()

  def setConf(brokers: String, groupId: String,
              pollCnt: Int = 10000, maxBytes: Int = 1073741824 /*1024 * 1024 * 1024*/ ,
              timeoutMs: String = "5000"): Unit = {
    //common conf
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    //    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true") //TODO
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")

    // brokers
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG.asInstanceOf[java.lang.Object],
      maxBytes.asInstanceOf[java.lang.Object])
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, pollCnt.asInstanceOf[java.lang.Object])
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, timeoutMs)
  }
}
