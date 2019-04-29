package com.learn.log

import java.sql._

/**
  * Created by kimvra on 2019-04-29
  */
object MySQLUtil extends App {
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/SparkProject?useSSL=false&user=root&password=admin")
  }

  def release(connection: Connection, pstmt: PreparedStatement) = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
