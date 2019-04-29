package com.learn.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by kimvra on 2019-04-27
  */
object ParquetApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    val userDataFrame = sparkSession.read.format("json").load("file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")

    userDataFrame.printSchema()
    userDataFrame.show()

    sparkSession.close()
  }

}
