package com.dangdang.util
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
    println(path + " RDD Size = " + lineRDD.count())
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
