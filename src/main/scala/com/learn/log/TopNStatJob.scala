package com.learn.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * Created by kimvra on 2019-04-29
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()


    val accessDataFrame = sparkSession.read.format("parquet").load("/Users/kimvra/IdeaProjects/imooc/cleanData")
    accessDataFrame.printSchema()
    accessDataFrame.show(false)

    // 最受欢迎Top N课程
    //videoAccessTopNStat(sparkSession, accessDataFrame)

    // 按照地市进行统计Top N课程
    cityAccessTopNStat(sparkSession, accessDataFrame)

    sparkSession.stop()
  }

  def cityAccessTopNStat(sparkSession: SparkSession, accessDataFrame: DataFrame) = {
    import sparkSession.implicits._
    val cityAccessDataFrame = accessDataFrame.filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))

    cityAccessDataFrame.show(false)

    val top3DataFrame = cityAccessDataFrame.select(cityAccessDataFrame("day"),
      cityAccessDataFrame("city"),
      cityAccessDataFrame("cmsId"),
      cityAccessDataFrame("times"),
      row_number().over(Window.partitionBy(cityAccessDataFrame("city")).orderBy(cityAccessDataFrame("times").desc))
        .as("times_rank")).filter("times_rank <= 3")

    top3DataFrame.show(false)

    try {
      top3DataFrame.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def videoAccessTopNStat(sparkSession: SparkSession, accessDataFrame: DataFrame) = {
    /*import sparkSession.implicits._
    var videoAccessDataFrame = accessDataFrame.filter($"day" === "20170511" && $"cmsType" === "video").groupBy("day", "cmsId")
      .agg(count("cmsId").as("times")).orderBy($"times".desc)
    videoAccessDataFrame.show(false)*/

    accessDataFrame.createOrReplaceTempView("access_logs")
    val videoAccessTopNDataFrame = sparkSession.sql("select day,cmsId,count(1) as times from access_logs where day='20170511' and cmsType='video' group by day,cmsId order by times desc")
    videoAccessTopNDataFrame.show(false)

    try {
      videoAccessTopNDataFrame.foreachPartition(partitionOdRecords => {
        val list = new ListBuffer[DayVideoAccess]
        partitionOdRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccess(day, cmsId, times))

        })
        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case exception: Exception => exception.printStackTrace()
    }

  }
}
