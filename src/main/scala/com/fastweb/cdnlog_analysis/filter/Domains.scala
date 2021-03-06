package com.fastweb.cdnlog_analysis.filter

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import java.util.zip.GZIPInputStream

import com.fastweb.cdnlog_analysis.Channel



object Domains {

  // 时间间隔为一天,一天更新一次cdn channel
  val timeinterval = 1 * 60 * 60 * 1000L

  var lastUpdateTime = System.currentTimeMillis()
  val userids = Set("1159")
  val domains = collection.mutable.Set[String]() ++ update()

  def apply(inStream: InputStream): Set[String] = {
    val result = collection.mutable.Set[String]()
    val channel = new Channel()
    var reader = new BufferedReader(new InputStreamReader(inStream))
    var tmp = ""
    while ( {
      tmp = reader.readLine()
      tmp
    } != null) {
      val ops = tmp.split(",")
      if (tmp.startsWith(Channel.EXT_PREFIX) && ops(2).equals("2")) {
        if (userids.contains(ops(6))) {
          result.add(ops(0).substring(4))
        }
      } else {
        if (userids.contains(ops(6))) {
          result.add(ops(0))
        }
      }
    }
    result.toSet
  }

  def apply(): Set[String] = {
    val currentTime = System.currentTimeMillis()
    if ((currentTime - lastUpdateTime) > timeinterval) {
      lastUpdateTime = currentTime
      domains.clear()
      domains ++= update()
    }
    domains.toSet
  }

  def update(): Set[String] = {
    val result = collection.mutable.Set[String]()
    val url = new URL(Channel.CDN_CHANNEL_URL)
    val gz = new GZIPInputStream(url.openStream())
    try {
      println("update")
      result ++= this(gz)
    } finally {
      gz.close()
    }
    result.toSet
  }
  def main(args: Array[String]) {
    println(Domains.apply())
    Thread.sleep(10000)
    println(Domains.apply())
  }
}