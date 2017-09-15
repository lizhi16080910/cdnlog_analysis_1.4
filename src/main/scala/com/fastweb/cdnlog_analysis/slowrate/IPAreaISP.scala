package com.fastweb.cdnlog_analysis.slowrate

import java.io.{BufferedReader, InputStreamReader}
import java.util

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Try

/**
  * Created by lfq on 2017/1/12.
  */
object IPAreaISP {
    // val CHANNEL_URL = "http://udap.fwlog.cachecn.net:8088/ip_list.gz";
    val ISP_PRV_NAME_IPS = "http://udap.fwlog.cachecn.net:8089/udap/isp_prv_name_ips"

   // val ISP_PRV_NAME_IPS = "http://202.85.220.117:8080/pub/ipbases/dnsopt/isp_prv_name_ips"

    // 时间间隔为一天,一天更新一次cdn channel
    val timeinterval = 48 * 60 * 60 * 1000L
    var lastUpdateTime = System.currentTimeMillis()

    val cityIP = new util.TreeMap[Long, Node]()
    init()

    //执行初始化操作

    def updateIspPrvNameIPS(): Unit = {
        val currentTime = System.currentTimeMillis()
        if ((currentTime - lastUpdateTime) > timeinterval) {
            cityIP.clear()
            Try {
                val source = Source.fromURL(ISP_PRV_NAME_IPS)
                source.getLines().foreach(line => {
                    val temp = line.split(" ")
                    val key = temp(0).trim.toLong
                    cityIP.put(key, new Node(key, temp(1).trim.toLong, temp(2).trim.toInt, temp(3).trim, temp(4).toInt, temp(5).trim))
                })
                source.close()
            }
        }
    }

    def init(): Unit = {
        var inStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("isp_prv_name_ips")
        val reader = new BufferedReader(new InputStreamReader(inStream))
        var tmp = ""
        cityIP.clear()
        while ( {
            tmp = reader.readLine(); tmp
        } != null) {
            if (!tmp.trim.equals("")) {
                val temp = tmp.split(" ")
                val key = temp(0).trim.toLong
                cityIP.put(key, new Node(key, temp(1).trim.toLong, temp(2).trim.toInt, temp(3).trim, temp(4).toInt, temp(5).trim))
            }
        }
        inStream.close()
    }

    def getNode(ip: Long): Node = {
        //updateIspPrvNameIPS()
        val node = cityIP.floorEntry(ip).getValue
        if (ip >= node.start && ip <= node.end) {
            node
        } else {
            new Node(0, 0, 0, "NULL", 0, "NULL")
        }
    }

    def main(args: Array[String]) {
        val cityIP = new util.TreeMap[Long, Node]()
        cityIP.foreach(r => println(r._1, r._2.start))

        //updateIspPrvNameIPS()
        //init()
        println(SlowRateLogInfo.ipToLong("10.104.33.252"))
        println(getNode(SlowRateLogInfo.ipToLong("10.104.33.252")))
    }
}
