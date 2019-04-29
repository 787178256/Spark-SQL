package com.learn.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by kimvra on 2019-04-20
  */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    // 将一个json文件加载成一个dataframe
    val peopleDF = sparkSession.read.format("json").load("file:///Users/kimvra/IdeaProjects/imooc/people.json")

    // 输出dataframe对应的schema信息
    peopleDF.printSchema()
    // 输出数据集的前20条数据
    peopleDF.show()
    // 查询某列的所有数据
    peopleDF.select("name").show()
    // 查询某列的所有数据，并对列进行计算
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show()
    // 根据某一列的值进行过滤
    peopleDF.filter(peopleDF.col("age") > 19).show()
    // 根据某一列进行分组，然后再进行聚合操作
    peopleDF.groupBy("age").count().show()

    sparkSession.stop()
  }
}
