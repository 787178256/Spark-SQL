package com.learn.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by kimvra on 2019-04-29
  */
object SparkStatCleanJobOnYARN {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: SparkStatCleanJobOnYARN <inputPath> <outputPath>")
      System.exit(1)
    }

    val Array(inputPath, outputPath) = args

    val sparkSession = SparkSession.builder().getOrCreate()
    val accessRDD = sparkSession.sparkContext.textFile(inputPath)

//    accessRDD.take(10).foreach(println)
    // RDD ==> DF
    val accessDataFrame = sparkSession.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)), AccessConvertUtil.struct)
//    accessDataFrame.printSchema()
//    accessDataFrame.show(false)
    accessDataFrame.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save(outputPath)
    sparkSession.stop()
  }
}
