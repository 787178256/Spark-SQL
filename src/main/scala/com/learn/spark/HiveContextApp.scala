package com.learn.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kimvra on 2019-04-20
  */
object HiveContextApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sparkContext)

    hiveContext.table("emp").show()

    sparkContext.stop()
  }

}
