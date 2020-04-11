package com.dangdang

import com.dangdang.util.TFileClientRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object hdfsDiff {

  def main(args: Array[String]): Unit = {
    if (args.length<4){
      println("args.length = %d".format(args.length))
      println("out1 out2 old new")
      return
    }

    val outFile_1 = args(0)
    val outFile_2 = args(1)
    val oldFile = args(2)
    val newFile = args(3)
//    val outFile_1 = "hdfs://10.5.25.148:8020/data/userData/old_new"
//    val outFile_2 = "hdfs://10.5.25.148:8020/data/userData/new_old"

    println("start....")

    val conf = new SparkConf().setAppName("hdfs diff")
    val sc = new SparkContext(conf)

    val tClient = new TFileClientRDD(sc)
    val oldRDD = tClient.load(oldFile, 0)
    val newRDD = tClient.load(newFile, 0)

    val oldResult = process(oldRDD)
    val newResult = process(newRDD)
    println("oldResult RDD Size = " + oldResult.count())
    println("newResult RDD Size = " + newResult.count())
    // 求并集 & 去重
    val unionRDD = oldResult.union(newResult).distinct()
    println("union Size = " + unionRDD.count())

    // 求交集
    val intersectionRDD = oldResult.intersection(newResult)
    println("intersection Size = " + intersectionRDD.count())

    // 求差集
    val subtractRDD = oldResult.subtract(newResult)
    println("old - new subtract Size = " + subtractRDD.count())
    if (subtractRDD.count() > 0) {
      tClient.save(outFile_1, 0, subtractRDD)
    }
    // 求差集2
    val subtractRDD2 = newResult.subtract(oldResult)
    println("new - old subtract Size = " + subtractRDD2.count())
    if (subtractRDD2.count() > 0) {
      tClient.save(outFile_2, 0, subtractRDD2)
    }

    println("end")
    sc.stop()
    return

  }

  def process(inRDD: RDD[String]): RDD[String] = {
    val resultRDD = inRDD.map(l => l.split("\t", -1))
      .filter(v => v.length > 17)
      .map(c => {
        val time = c(0)
        val ip = c(1)
        val perm = c(3)
        val adid = c(4)
        val rdm = c(16)

        (time + "\t" + ip + "\t" + perm + "\t" + adid + "\t" + rdm)
      })
    resultRDD.persist()
    inRDD.unpersist()

    resultRDD
  }
}
