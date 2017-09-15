package com.fastweb.cdnlog_analysis

import scala.annotation.tailrec
import java.text.SimpleDateFormat
import org.apache.commons.codec.binary.Base64
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.util.Locale

case class DirCs(domain: String, dir1: String, dir2: String, timestmp: String, cs: Long, pno: String, isp: String, prv: String) extends Equals {
  def canEqual(other: Any) = {
    other.hashCode() == this.hashCode()
  }

  override def equals(other: Any) = {
    other match {
      case that: com.fastweb.cdnlog_analysis.DirCs => that.canEqual(DirCs.this)
      case _                                       => false
    }
  }

  override def hashCode() = {
    (domain + dir1 + dir2 + timestmp).hashCode()
  }
}

object DirCs {

  def empty() = { null }

  def apply(line: String, channel: Channel): DirCs = {
    //    var dirCsDomainList = DirCsDomainConf.update()
    var startIndex = line.indexOf("://")
    if (startIndex < 0) {
      return empty();
    }

    var endIndex = line.indexOf("/", startIndex + 3)
    if (endIndex < 0) {
      return empty();
    }
    val domain = line.substring(startIndex + "://".length(), endIndex)
    /*
    if (!dirCsDomainList.contains(domain)) {
      return empty(line);
    }
    */
    var logInfo = LogInfo(line, channel)
    if (logInfo == null) {
      return empty();
    }

    var url = logInfo.url
    var index = url.indexOf("/")
    index = index
    var pre = 0
    var end = false
    def next(): String = {
      if (end) {
        return "-"
      }
      pre = index + 1
      index = url.indexOf("/", pre)
      if (index < pre) {
        end = true
        return "-"
      }
      url.substring(pre, index)
    }
    DirCs(logInfo.domain, next(), next(), logInfo.timestmp.substring(0, 12), logInfo.cs, "", logInfo.isp, logInfo.prv)
  }

  def timestamp2Date(timestamp: String): Long = {
    val formate = new SimpleDateFormat("yyyyMMddHHmm", Locale.SIMPLIFIED_CHINESE)
    if (timestamp == null || timestamp.equals("")) {
      0
    }
    try {
      formate.parse(timestamp).getTime / 1000
    } catch {
      case e: Throwable => 0
    }
  }
  def main(args: Array[String]) {
    
    def printLine(line: Any){
      val clazz = line.getClass
      val arr = clazz.getDeclaredFields()
      for(i <- 0 to arr.length-1){
        val f = arr(i)
        f.setAccessible(true)
        print(f.getName + "：" + f.get(line).toString() + "\t")
      }
      println()
    }
    
    val arr = Source.fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("powza")).getLines()
    for(line <- arr){
      val dir = DirCs(line, Channel.update())
      printLine(dir)
    }
  }

}