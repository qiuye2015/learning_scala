package com.dangdang

/**
  * @author ${user.name}
  */
//java -cp user-1.0-jar-with-dependencies.jar  com.dangdang.App
//object App {
//
//  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
//
//  def main(args : Array[String]) {
//    println( "Hello World!" )
//    println("concat arguments = " + foo(args))
//  }
//
//}

import scala.io.Source

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object UserLeave {
  val tabDelimiter: String = "\t"

  var vco_data_oday_path = ""
  var quert_path = ""
  var eventID_path = ""
  var app_hdfs = ""
  var out_hdfs = ""
  var before_int = 1
  var after_int = 2

  def main(args: Array[String]): Unit = {

    for (i <- 0 until args.length) {
      println(args(i))
    }
    if (args.length < 5) {
      println("args.length = %d".format(args.length))
      return
    }
    vco_data_oday_path = args(0)
    quert_path = args(1)
    eventID_path = args(2)
    app_hdfs = args(3)
    out_hdfs = args(4)

    if (args.length >= 7) {
      before_int = SafeConvert.int(args(5))
      after_int = SafeConvert.int(args(6))
    }

    //前1分钟和后2分钟
    val before = 60 * 1000 * before_int
    val after = 60 * 1000 * after_int

    val prefix = Time.getTime.toString
    out_hdfs += prefix

    println("main start")
    val sc = new SparkContext()
    val tClient = new TFileClientRDD(sc)

    val queryFile = Source.fromFile(quert_path, "UTF-8")
    val queryArray = queryFile.getLines().toArray

    val RDD = tClient.load(vco_data_oday_path, 10)
    val appRDD = tClient.load(app_hdfs, 10)
    val eventIDMap = tClient.load(eventID_path, 0)
      .map(l => l.split("\t", -1))
      .filter(l => l.length == 2)
      .map(x => (x(0), x(1)))
      .collect
      .toMap

    val appColSize: Int = 59

    val query2permidRDD1 = RDD.map(l => l.split(tabDelimiter, -1))
      .filter(l => l.length >= 10)
      .filter(l => l(0).nonEmpty) //permid不为空
      .map(cols => {
      val permid = cols(1)
      val query = cols(2)
      val time = cols(6) //2019-08-13 19:20:38
      val bounce = cols(9)
      (permid, time, query, bounce, "", "", "", "")
    })
      .filter(l => l._4 == "1" && queryArray.contains(l._3)) //有跳出行为的用户
      .sortBy(l => (l._3, l._1, l._2))

    query2permidRDD1.persist()

    val query2permidRDD = query2permidRDD1
      .map(l => {
        (l._3 + tabDelimiter + l._1 + tabDelimiter + l._2)
      })
    tClient.save(out_hdfs, 0, query2permidRDD)

    val permid2timeMap = query2permidRDD1
      .map(cols => {
        val permid = cols._1
        val time = stringToTimestamp(cols._2)
        (permid, time)
      })
      .map(t => (t._1, List(t._2)))
      .reduceByKey(_ ::: _)
      .collect().toMap

    println(permid2timeMap.size)

    val name2colMap = Map(
      "creation_date" -> 28,
      "cust_id" -> 30,
      "pageid" -> 31,
      "eventid" -> 32,
      "app_ip" -> 36,
      "linkurl" -> 41,
      "click_content" -> 42,
      "refer_url" -> 44,
      "permanent_id" -> 46,
      "searchword" -> 57
    )

    val perm2eventRDD = appRDD.map(l => l.split(tabDelimiter))
      .filter(l => l.length >= appColSize)
      .map(cols => {
        val creation_date = cols(name2colMap("creation_date"))
        val pageid = cols(name2colMap("pageid"))
        val eventid = cols(name2colMap("eventid"))
        val linkurl = cols(name2colMap("linkurl"))
        val permanent_id = cols(name2colMap("permanent_id"))
        val click_content = cols(name2colMap("click_content"))
        val refer_url = cols(name2colMap("refer_url"))

        val eventidStr = eventIDMap.getOrElse(eventid, "")

        (permanent_id, creation_date, pageid, eventid, eventidStr, linkurl, click_content, refer_url)
      }) // pageid=1038 搜索页  pageid=1020为搜索中间页 4002为搜索
      .filter(l => l._1.nonEmpty && (l._3 == "1038" || (l._3 == "1020" && l._4 == "4002"))) //permid不为空，即使跳出率中有空的
      .filter(l => {
      val permid = l._1
      val time = stringToTimestamp(l._2) // x-1 <time < x+2
      val eventidStr = l._5
      eventidStr.nonEmpty &&
        permid2timeMap.contains(permid) && {
        var flag: Boolean = false
        val timeArray = permid2timeMap.getOrElse(permid, List())
        timeArray.foreach(x => {
          flag = flag || ((time >= (x - before)) && (time <= (x + after)))
        })
        flag
      }
    })
      .sortBy(l => (l._1, l._2))
    perm2eventRDD.persist()

    val resultRDD = perm2eventRDD
      .map(l => {
        l._1 + tabDelimiter + l._2 + tabDelimiter + l._3 + tabDelimiter + l._4 + tabDelimiter + l._5 + tabDelimiter + l._6 + tabDelimiter + l._7 + tabDelimiter + l._8
      })
      //uniq start
      .flatMap(line => line.split("\n"))
      .map(a => (a, 1))
      .groupByKey()
      .sortByKey()
      .keys
    //uniq end
    tClient.save(out_hdfs + "res", 0, resultRDD)

    val mergeRDD = perm2eventRDD.union(query2permidRDD1).sortBy(l => (l._1, l._2)).map(l => {
      (l._1 + tabDelimiter + l._2 + tabDelimiter + l._3 + tabDelimiter + l._4 + tabDelimiter + l._5 + tabDelimiter + l._6 + tabDelimiter + l._7 + tabDelimiter + l._8)
    })

    tClient.save(out_hdfs + "merge", 0, mergeRDD)

    sc.stop()
    return
  }

  /**
    * 普通时间转时间戳
    *
    * @param dateStr 输入时间 2019-07-22 00:00:00
    * @param pattern 输入时间格式
    * @return 13位时间戳 1563724800266
    */
  def stringToTimestamp(dateStr: String,
                        pattern: String = "yyyy-MM-dd HH:mm:ss")
  : Long = {
    try {
      new SimpleDateFormat(pattern, Locale.SIMPLIFIED_CHINESE).parse(dateStr).getTime
    } catch {
      case ex: java.text.ParseException => 0
    }
  }

}

class TFileClientRDD(sc: SparkContext) {

  def load(path: String, part: Int)
  : RDD[String] = {

    val lineRDD = if (part > 0) {
      sc
        .textFile(path)
        .repartition(part)
        .persist()
    } else {
      sc
        .textFile(path)
        .persist()
    }

    /* persist RDD, caller should un-persist. */
    lineRDD
  }

  def save(path: String, part: Int, lineRDD: RDD[String])
  : Unit = {

    lineRDD.persist()
    if (part > 0) {
      lineRDD.repartition(part).saveAsTextFile(path)
    } else {
      lineRDD.saveAsTextFile(path)
    }
    lineRDD.unpersist()
  }
}

object SafeConvert {

  /* safe string conversion */

  def string(s: String)
  : String = {
    if (s == null) {
      ""
    } else {
      s
    }
  }

  def int(s: String)
  : Int = {
    if (s == null) {
      0
    } else {
      try {
        s.toInt
      } catch {
        case _: Exception => 0
      }
    }
  }

  def long(s: String)
  : Long = {
    if (s == null) {
      0L
    } else {
      try {
        s.toLong
      } catch {
        case _: Exception => 0L
      }
    }
  }

  def float(s: String)
  : Float = {
    if (s == null) {
      0F
    } else {
      try {
        s.toFloat
      } catch {
        case _: Exception => 0F
      }
    }
  }

  def sub(c: Long, s: Long)
  : Long = {
    if (c > s) {
      c - s
    } else {
      0L
    }
  }

  def div(c: Long, b: Long)
  : Long = {
    if (b > 0) {
      c / b
    } else {
      0L
    }
  }

  def rate(c: Long, b: Long)
  : Float = {
    if (b > 0) {
      if (b > c) {
        c.toFloat / b
      } else {
        1F
      }
    } else {
      0F
    }
  }

  def norm(c: Long, l: Long, h: Long)
  : Float = {
    if ((h > l) && (c > l)) {
      if (h > c) {
        (c - l).toFloat / (h - l)
      } else {
        1F
      }
    } else {
      0F
    }
  }

  def norm(c: Float, l: Float, h: Float)
  : Float = {
    if ((h > l) && (c > l)) {
      if (h > c) {
        (c - l) / (h - l)
      }
      else {
        1F
      }
    } else {
      0F
    }
  }
}

object Time {

  def getTime
  : Long = {
    val date = new Date
    date.getTime
  }
}