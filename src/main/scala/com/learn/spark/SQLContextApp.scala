package com.learn.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by kimvra on 2019-04-10
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {
    val path = args(0)

    val sparkConf = new SparkConf()
    //sparkConf.setAppName("SQLContextAPP").setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    sparkContext.stop()
  }
}
