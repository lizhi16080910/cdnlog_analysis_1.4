package com.fastweb.cdnlog_analysis

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URL
import java.util.HashSet
import java.util.zip.GZIPInputStream
import org.apache.commons.logging.LogFactory
import java.util.ArrayList

class UaConfUtil {

  val uaList = new ArrayList[String]

  def getUaType(uaString: String): String = {
    val ua_l = uaString.toLowerCase()
    var i = 0
    while(i< uaList.size()){
      val ua = uaList.get(i)
      i = i + 1
      if(ua_l.contains(ua)){
        return ua;
      }
    }
    return "other"
  }
}

object UaConfUtil {

  val log = LogFactory.getLog(getClass());

  val CDN_UA_CONF = "http://udap.fwlog.cachecn.net:8088/ua.conf"

  // 时间间隔为天,一天更新一次过滤ua列表
  val timeinterval = 1 * 24 * 60 * 60 * 1000L

  var lastUpdate = System.currentTimeMillis()

  var current = apply(CDN_UA_CONF)

  def apply(inStream: InputStream): UaConfUtil = {
    val uaConf = new UaConfUtil()
    var reader = new BufferedReader(new InputStreamReader(inStream))
    var tmp = ""
    while ({ tmp = reader.readLine(); tmp } != null) {
      println("----------------------------" + tmp + "-----------------------------")
      val uaArray = tmp.trim().split(",")
      for (ua <- uaArray) {
        uaConf.uaList.add(ua)
      }
    }
    uaConf
  }

  def apply(path: String): UaConfUtil = {
    var url = new URL(path)
    try {
      val in = url.openStream()
      try {
        current = this(in)
        return current
      } catch {
        case e: Throwable => {
          println(e)
          return current
        }
      } finally {
        in.close()
      }
    } catch {
      case e: Throwable => {
        println(e)
        return current
      }
    }
  }

  def apply(): UaConfUtil = {
    var inStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("ua.conf")
    try {
      this(inStream)
    } finally {
      inStream.close()
    }
  }
  def update(): UaConfUtil = {
    val currentTime = System.currentTimeMillis()
    if ((currentTime - lastUpdate) > timeinterval) {
      lastUpdate = currentTime
      apply(CDN_UA_CONF)
    } else {
      current
    }
  }

  def main(args: Array[String]) {
    println(UaConfUtil.update().getUaType(""))
  }
}