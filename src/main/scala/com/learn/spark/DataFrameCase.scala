package com.learn.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by kimvra on 2019-04-22
  * DataFrame中的其他操作
  */
object DataFrameCase {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    val rdd = sparkSession.sparkContext.textFile("file:///Users/kimvra/IdeaProjects/imooc/student.data")
    import sparkSession.implicits._
    val studentDataFrame = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    // show方法默认显示前20条记录
    //studentDataFrame.show()
    //studentDataFrame.show(30, false)
    studentDataFrame.take(10).foreach(println)
    sparkSession.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)
}
