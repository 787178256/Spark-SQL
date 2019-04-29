package com.learn.spark

import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
/**
  * Created by kimvra on 2019-04-21
  */
object DFRDDApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    // inferReflection(sparkSession)
    program(sparkSession)

  }

  case class Info(id: Int, name: String, age: Int)

  /**
    * 编程的方式
    * @param sparkSession
    */
  def program(sparkSession: SparkSession): Unit = {
    val rdd = sparkSession.sparkContext.textFile("file:///Users/kimvra/IdeaProjects/imooc/infos.txt")
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))
    val structType = StructType(Array(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("age", IntegerType, true)))

    val infoDataFrame = sparkSession.createDataFrame(infoRDD, structType)
    infoDataFrame.printSchema()
    infoDataFrame.show()
    infoDataFrame.filter(infoDataFrame.col("age") > 30).show()
    infoDataFrame.createOrReplaceTempView("infos")
    sparkSession.sql("select * from infos where age > 30").show()

  }


  /**
    * 反射的方式，需要提前知道字段、字段类型
    * @param sparkSession
    */
  def inferReflection(sparkSession: SparkSession): Unit = {
    val rdd = sparkSession.sparkContext.textFile("file:///Users/kimvra/IdeaProjects/imooc/infos.txt")
    import sparkSession.implicits._
    val infoDataFrame = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    infoDataFrame.show()
    infoDataFrame.filter(infoDataFrame.col("age") > 30).show()

    infoDataFrame.createOrReplaceTempView("infos")
    sparkSession.sql("select * from infos where age > 30").show()

    sparkSession.stop()
  }
}
