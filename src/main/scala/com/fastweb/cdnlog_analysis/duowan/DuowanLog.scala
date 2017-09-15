package com.fastweb.cdnlog_analysis.duowan

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.log4j.Logger

/**
 * Created by zhangyi on 2016/8/17.
 */
case class DuowanLog(domain:String,dir:String, timestamp:Long, flow_size:Long,https_flag:String)

object DuowanLog {

  private[this] val log = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val line4 = "CDNLOG cnc-jl-175-022-002-182 dl.vip.yy.com#_#139.214.117.15#_#-#_#[22/Aug/2016:16:13:03 +0800]#_#GET /vipface/20140411.7z HTTP/1.1#_#200#_#1148235#_#-#_#-#_#731.111#_#0.000#_#175.22.2.209#_#TCP_HIT#_#fastweb#_#1148591#_#http 175.22.2.182"

    val line2="CDNLOG ctl-fj-218-006-023-025 aipai.w5.dwstatic.com#_#222.77.194.49#_#-#_#[22/Aug/2016:16:13:16 +0800]#_#GET /52/7/1634/3055099-139-1471847137.mp4 HTTP/1.1#_#206#_#6806520#_#http://m.aipai.com/m31/OT07JCYmJCdpJGsp.html#_#Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_4 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G35 QQ/6.5.3.410 V1_IPH_SQ_6.5.3_1_APP_A Pixel/640 Core/UIWebView NetType/WIFI Mem/64#_#2436.875#_#0.000#_#218.6.23.25#_#TCP_HIT#_#fastweb#_#6807173#_#https 218.6.23.25"


    val line3="CDNLOG_cache gwb-hb-211-161-124-023 dl.vip.yy.com#_#14.103.27.197#_#-#_#[23/Aug/2016:18:14:04 +0800]#_#GET http://dl.vip.yy.com/icons/minicard_vip_prettynum_14x_6.png HTTP/1.1#_#200#_#1194#_#-#_#-#_#0.000#_#0.000#_#211.161.124.7#_#TCP_HIT#_#fastweb#_#1620 21"

    val line="@|@c06.i06@|@68@|@CTL_Taizhou2_74_89@|@CDNLOG_cache ctl-js-061-147-218-018 @|@00000000@|@dl.vip.yy.com#_#61.191.254.226#_#-#_#[09/Nov/2016:08:58:28 +0800]#_#GET http://dl.vip.yy.com/vipskin/icon/32.png HTTP/1.1#_#200#_#8241#_#-#_#-#_#0.000#_#0.000#_#59.63.188.153#_#TCP_HIT-#_#fastweb#_#8667 59.63.188.151"

    for (line <- List(line,line2, line3,line4))
    {
      println(parseLine(line))

    }

  }

  def apply(line: String): DuowanLog = {
    try {
      parseLine(line)
    } catch {
      case e: Throwable => {
        log.error("not valid log: " + line, e)
        null
      }
    }
  }

  def parseLine(line: String): DuowanLog = {

    if(!line.contains("#_#")){
      return null
    }
    //获取时间
    val timeFormat = "dd/MMM/yyyy:HH:mm:ss Z"

    //判断眼睛日志
    if (line.startsWith("@|@")) {
      val logArraytemp = line.split("@\\|@")
      val line1 = logArraytemp(4) + logArraytemp(6)

      //获取时间
      val logArray = line1.split("#_#")

      val ip = logArray(1)
      if (ip.startsWith("127.0.0")) {
        return null
      }

      val domain = logArray(0).split(" ")(2)
      val dirStr = logArray(4).split(" ")(1).split("/")


      //file uri
      var dir = "/" + dirStr(1)

      //http uri
      if (dirStr(0).equals("http:")) {
        //dir = dirStr(0) + "/" + dirStr(1) + "/" + dirStr(2) + "/" + dirStr(3)
        dir = "/" + dirStr(3)
      }

      val cs = logArray(14).split(" ")(0).toLong
      val timestmp = logArray(3).replace("[", "").replace("]", "")

      val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
      val time = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(timestmp))

      //判断是否为https访问请求
      var https_flag = "-"
      if (logArray(logArray.length - 1).startsWith("https")) {
        https_flag = "https"
      }
      else {
        https_flag = "http"
      }

      return DuowanLog(domain, dir, getFiveTimestampBySecondTimestamp(timeToLong(time)), cs,https_flag)
    }



    val logArray = line.split("#_#")

    val ip = logArray(1)
    if ("127.0.0.1".equals(ip)) {
      return null
    }

    val domain = logArray(0).split(" ")(2)
    val dirStr = logArray(4).split(" ")(1).split("/")


    //file uri
    var dir = "/" + dirStr(1)

    //http uri
    if (dirStr(0).equals("http:")) {
      //dir = dirStr(0) + "/" + dirStr(1) + "/" + dirStr(2) + "/" + dirStr(3)
      dir = "/" + dirStr(3)
    }

    val cs = logArray(14).split(" ")(0).toLong
    val timestmp = logArray(3).replace("[", "").replace("]", "")

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val time = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(timestmp))

    //判断是否为https访问请求
    var https_flag = ""
    if (logArray(logArray.length - 1).startsWith("https")) {
       https_flag = "https"
    }
    else {
       https_flag = "http"
    }


    return DuowanLog(domain, dir, getFiveTimestampBySecondTimestamp(timeToLong(time)), cs,https_flag)
  }

  def getFiveTimestampBySecondTimestamp(secondTimestamp: Long): Long = {
    try {
      (secondTimestamp / 300 * 300 + 300)
    } catch {
      case _: Throwable => secondTimestamp
    }
  }

  def timeToLong(time: String): Long = {
    val format = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
    format.parse(time).getTime / 1000
  }
}
