package com.learn.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by kimvra on 2019-04-20
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("SparkSessionApp").master("local[2]")getOrCreate()

    //sparkSession.read.format("json")
    val people = sparkSession.read.json("file:///Users/kimvra/IdeaProjects/imooc/people.json")
    people.show()

    sparkSession.stop()
  }
}
