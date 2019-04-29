package com.learn.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by kimvra on 2019-04-27
  */
object HiveMySQLApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("HiveMySQLApp").master("local[2]").getOrCreate()
    val hiveDataFrame = sparkSession.table("emp")
    val mysqlDataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("driver", "com.mysql.jdbc.driver")
      .option("dbtable", "spark.DEPT")
      .option("user", "root")
        .option("password", "admin")
        .load()

    val resultDataFrame = hiveDataFrame.join(mysqlDataFrame, hiveDataFrame.col("deptno") === mysqlDataFrame.col("DEPTNO"))
    resultDataFrame.show()

    sparkSession.close()
  }

}
