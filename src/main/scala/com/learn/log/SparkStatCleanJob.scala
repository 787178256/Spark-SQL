package com.learn.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by kimvra on 2019-04-29
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()
    val accessRDD = sparkSession.sparkContext.textFile("file:///Users/kimvra/IdeaProjects/imooc/data/access.log")

//    accessRDD.take(10).foreach(println)
    // RDD ==> DF
    val accessDataFrame = sparkSession.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)), AccessConvertUtil.struct)
//    accessDataFrame.printSchema()
//    accessDataFrame.show(false)
    accessDataFrame.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("/Users/kimvra/IdeaProjects/imooc/cleanData")
    sparkSession.stop()
  }
}
