package com.fastweb.cdnlog_analysis.boss

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URL
import java.util.HashSet
import java.util.zip.GZIPInputStream

import org.apache.commons.logging.LogFactory

class FilterUserUtils {

  /**
   * 存储 需要 计算的 用户ID
   */
  val userids = new HashSet[String]

  def checkUserID(userid: String): Boolean = {
    if (userids.contains(userid)) true else false
  }
}

object FilterUserUtils {

  val log = LogFactory.getLog(getClass());

  val CDN_USERID = "http://udap.fwlog.cachecn.net:8088/cdn_filter_userid.gz"

  // 时间间隔为小时,一小时更新一次过滤userid列表
  val timeinterval = 1 * 60 * 60 * 1000L

  var lastUpdate = System.currentTimeMillis()

  var current = apply(CDN_USERID)

  def apply(inStream: InputStream): FilterUserUtils = {
    val users = new FilterUserUtils()
    var reader = new BufferedReader(new InputStreamReader(inStream))
    var tmp = ""
    while ({ tmp = reader.readLine(); tmp } != null) {
      log.info("----------------------------" + tmp + "-----------------------------")
      val useridArray = tmp.trim().split(",")
      for (userId <- useridArray) {
        users.userids.add(userId)
      }
    }
    users
  }

  def apply(path: String): FilterUserUtils = {
    var url = new URL(path)
    try {
      val gz = new GZIPInputStream(url.openStream())
      try {
        current = this(gz)
        return current
      } catch {
        case e: Throwable => {
          println(e)
          return current
        }
      } finally {
        gz.close()
      }
    } catch {
      case e: Throwable => {
        println(e)
        return current
      }
    }
  }

  def apply(): FilterUserUtils = {
    var inStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("cdn_filter_userid")
    try {
      this(inStream)
    } finally {
      inStream.close()
    }
  }
  def update(): FilterUserUtils = {
    val currentTime = System.currentTimeMillis()
    if ((currentTime - lastUpdate) > timeinterval) {
      lastUpdate = currentTime
      apply(CDN_USERID)
    } else {
      current
    }
  }

  def main(args: Array[String]) {
    println(FilterUserUtils.update().checkUserID("440"))
  }
}