package com.learn.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * Created by kimvra on 2019-04-29
  */
object StatDAO {

  def insertData(list: ListBuffer[Data]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtil.getConnection()
      connection.setAutoCommit(false)

      val sql = "insert into data(day, city, cms_id, times) values (?,?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setString(2, ele.city)
        pstmt.setLong(3, ele.cmsId)
        pstmt.setLong(4, ele.times)

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

  def insertDayVideoTrafficsAccessTopN(list: ListBuffer[DayVideoTrafficsStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null;

    try {
      connection = MySQLUtil.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_traffics_topn_stat(day, cmsId, traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)
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

  def deleteByDate(day: String) = {
    val tables = Array("day_video_access_topn_stat", "day_video_city_access_topn_stat", "day_video_traffics_topn_stat")

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtil.getConnection()

      for (table <- tables) {
        val deleteSQL = s"delete from $table where day = ?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1, day)
        pstmt.executeUpdate()
      }
    } catch {
      case exception: Exception => exception.printStackTrace()
    } finally {
      MySQLUtil.release(connection, pstmt)
    }
  }
}
