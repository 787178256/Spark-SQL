package com.learn.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by kimvra on 2019-04-22
  */
object DataSetApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DataSetApp").master("local[2]").getOrCreate()
    val path = "file:///Users/kimvra/IdeaProjects/imooc/sales.csv"
    val dataFrame = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(path)
    dataFrame.show()

    import sparkSession.implicits._
    val dataSet = dataFrame.as[Sales]
    dataSet.map(line => line.itemId).show()

    sparkSession.stop()
  }

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)
}
