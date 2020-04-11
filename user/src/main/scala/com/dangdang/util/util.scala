package com.dangdang.util

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

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