package com.learn.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * Created by kimvra on 2019-04-29
  */
object StatDAO {
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccess]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtil.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_access_topn_stat(day, cms_id, times) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      connection.commit()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(connection, pstmt)
    }
  }

  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtil.getConnection()
      connection.setAutoCommit(false)

      val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5, ele.timesRank)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case exception: Exception => exception.printStackTrace()
    } finally {
      MySQLUtil.release(connection, pstmt)
    }
  }
}
