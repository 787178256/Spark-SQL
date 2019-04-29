package com.learn.log

import org.apache.spark.sql.SparkSession

/**
  * Created by kimvra on 2019-04-29
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    val accessRDD = sparkSession.sparkContext.textFile("file:///Users/kimvra/IdeaProjects/imooc/data/10000_access.log")
//    accessRDD.take(10).foreach(println)
    accessRDD.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)
//      (ip, DateUtil.parse(time), url, traffic)
      DateUtil.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("file:///Users/kimvra/IdeaProjects/imooc/data/output")
    sparkSession.close()
  }
}
