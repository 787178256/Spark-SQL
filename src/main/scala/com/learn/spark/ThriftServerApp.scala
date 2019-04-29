package com.learn.spark

import java.sql.DriverManager

/**
  * Created by kimvra on 2019-04-20
  */
object ThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val connection = DriverManager.getConnection("jdbc:hive2://hadoop001:10000", "hadoop", "")
    val statement = connection.prepareStatement("select empno, ename, sal from emp")
    val resultSet = statement.executeQuery()
    while (resultSet.next()) {
      println("empno:" + resultSet.getInt("empno") + ", ename:" + resultSet.getString("ename")
       + ", sal:" + resultSet.getDouble("sal"))
    }

    resultSet.close()
    statement.close()
    connection.close()
  }
}
