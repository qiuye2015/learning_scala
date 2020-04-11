package com.dangdang.kafka

import com.dangdang.util.SafeConvert

//java -cp user-1.0-SNAPSHOT-jar-with-dependencies.jar com.dangdang.kafka.kafkaTest
object kafkaTest {
  val tab = "|"
  val state_dir = "./"
  val file_prefix = "test"

  def main(args: Array[String]): Unit = {

    var platform = "app"
    if (args.length == 1) {
      platform = args(0)
    }

    /*默认app*/
    var brokers = "10.3.17.171:9092"
    var groupId = "test"
    var topic = "mobile_client"
    var partitons = "0,1,2,3,4,5,6,7,8,9"

    if ("pc" == platform) {
      /*PC*/
      brokers = "10.3.17.171:9092"
      groupId = "test"
      topic = "load_click"
      partitons = "0,1,2,3,4,5,6,7,8,9"
    } else if ("miniapp" == platform) {
      /*miniapp*/
      var brokers = "10.4.83.38:9092"
      var groupId = "test"
      var topic = "miniprogram"
      var partitons = "0,1"
    }

    val kafkaConsumerConfig = new consumerConfig()
    kafkaConsumerConfig.setConf(brokers, groupId)

    val kafkaConsumerClient = new ConsumerClient(kafkaConsumerConfig.props, topic,
      partitons.split(",", -1).map(r => SafeConvert.int(r)).toSet)

    kafkaConsumerClient.initConsumer()
    while (true) {
      val data = kafkaConsumerClient.consumeData(10)
      data.foreach(x => {
        val vec = x.split("\t", -1)

        if (platform == "pc" && vec.length > 16) {
          /* PC*/
          /*permanent_id tpye time to_url cur_url refer_url*/
          println(vec(16) + tab + vec(7) + tab + vec(9) + tab +
            "to: " + vec(5) + tab + "cur: " + vec(3) + tab + "refer: " + vec(4))
        } else if ("miniapp" == platform && vec.length > 7) {
          /*miniapp*/
          /*permanent_id datatype time to_url cur_url refer_url*/
          println(vec(7) + tab + vec(3) + tab + vec(1) + tab +
            "to: " + vec(6) + tab + "cur: " + vec(4) + tab + "refer: " + vec(5))
        }
        else if (vec.length > 17) {
          /* APP*/
          /*permanent_id page_id eventID time to_url cur_url refer_url*/
          println(vec(17) + tab + vec(4) + tab + vec(5) + tab + vec(1) + tab +
            "to: " + vec(12) + tab + "cur: " + vec(9) + tab + "refer: " + vec(14))
        }
      })
      /*Thread.sleep(1000)*/
    }

  }
}
