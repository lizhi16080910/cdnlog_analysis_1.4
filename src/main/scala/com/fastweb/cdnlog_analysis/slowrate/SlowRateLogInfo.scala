package com.fastweb.cdnlog_analysis.slowrate

import java.util.Locale

import org.apache.spark.streaming.kafka.filter.slowrate.SlowRateFilterKafkaReceiver

import scala.util.{Failure, Success, Try}

/**
  * Created by lfq on 2017/1/11.
  */
/**
  *
  * @param domain
  * @param host
  * @param isp
  * @param province
  * @param time
  * @param ifSlow 1，表示慢速，0：表示正常
  */
case class SlowRateLogInfo(val domain: String, val host: String, val isp: String, val province: String, val time: Long,
                val ifSlow: Int) {
    //ifSlow：
}

//  cdnlog.rate.slow.percent


object SlowRateLogInfo {
    val timeThreshold = 0
    //1 秒
    val sizeThreshold = 256 // 200KB
    val speedThreshold = 100 // 100KB

    def main(args: Array[String]) {
        val log1 = "@|@c01.i01@|@17@|@CNC_Taiyuan_108_157@|@CDNLOG_cache cnc-sx-221-204-202-073 @|@00000000@|@171.125" +
            ".81.236 61 0 [11/Jan/2017:16:08:30 +0800] \"GET http://kwmov.a.yximgs.com/upic/2017/01/10/20/BMjAxNzAxMTAyMDA3MzVfMTI0NzkxOTUyXzE0ODk5Nzg0NDZfMV8z.mp4?tag=1-1484122003-h-0-vtic74t5zf-fa02dbd1a0c71842 HTTP/1.1\" 200 963384 \"-\" \"kwai-android\" mem 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000 61 1 - _ 171.125.81.236 - 963028 122.228.96.104";
        val log2 = "CDNLOG_cache cnc-he-060-008-151-247 61.181.5.162 5945.181 - [11/Jan/2017:16:08:48 +0800] \"GET http://kwmov.a.yximgs.com/upic/2017/01/10/20/BMjAxNzAxMTAyMDMyNDFfNDIzMjM4NTNfMTQ5MDExNTg0Nl8xXzM=.mp4?tag=1-1484122120-p-0-nu7iwwtlqs-732b91adc2567a22 HTTP/1.1\" 200 5823950 \"-\" \"kwai-android\" FCACHE_HIT_DISK 0.000 0.000 - - - - 0.000 5945.181 5945.181 1 - - 61.181.5.162 DISK HIT from 60.8.151.247 5823534 122.228.96.104";
        val log4 = "CDNLOG_cache cnc-he-060-008-151-247 61.181.5.162 5945.181 - [11/Jan/2017:16:08:48 +0800] \"GET " +
            "http://kwmov.a.yximgs.com/upic/2017/01/10/20/BMjAxNzAxMTAyMDMyNDFfNDIzMjM4NTNfMTQ5MDExNTg0Nl8xXzM=.mp4?tag=1-1484122120-p-0-nu7iwwtlqs-732b91adc2567a22 HTTP/1.1\" 200 5823950 \"-\" \"kwai-android\" FCACHE_HIT_DISK 0.000 0.000 - - - - 0.000 5945.181 5945.181 1 - - 61.181.5.162 DISK HIT from 60.8.151.247 5823534 122.228.96.104";

        val log3 = "@|@c01.i01@|@17@|@CNC_Taiyuan_108_157@|@CDNLOG_cache ctl-jx-059-063-188-151 @|@00000000@|@59.63.188.187" +
            " 1001 0 [11/Jan/2017:16:08:30 +0800] \"GET http://kwmo.a.yximgs" +
            ".com/upic/2017/01/10/20/BMjAxNzAxMTAyMDA3MzVfMTI0NzkxOTUyXzE0ODk5Nzg0NDZfMV8z.mp4?tag=1-1484122003-h-0-vtic74t5zf-fa02dbd1a0c71842 HTTP/1.1\" 200 963384 \"-\" \"kwai-android\" mem 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000 61 1 - _ 171.125.81.236 - 963028 59.63.188.151";
        val log5 = "@|@c01.i01@|@17@|@CNC_Taiyuan_108_157@|@CDNLOG_cache ctl-jx-059-063-188-151 @|@00000000@|@127.0.0" +
            ".1" +
            " 1001 0 [11/Jan/2017:16:08:30 +0800] \"GET http://kwmo.a.yximgs" +
            ".com/upic/2017/01/10/20/BMjAxNzAxMTAyMDA3MzVfMTI0NzkxOTUyXzE0ODk5Nzg0NDZfMV8z.mp4?tag=1-1484122003-h-0-vtic74t5zf-fa02dbd1a0c71842 HTTP/1.1\" 200 963384 \"-\" \"kwai-android\" mem 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000 61 1 - _ 171.125.81.236 - 963028 59.63.188.151";

        val log6 = "CDNLOG_cache ctl-jx-059-063-188-183 111.79.129.22 1058 0 [13/Jan/2017:12:59:03 +0800] \"GET " +
            "http://kwmov.a.yximgs" +
            ".com/upic/2017/01/08/10/BMjAxNzAxMDgxMDQ1MzZfMzcyNTE3OTY3XzE0NzgzMjQyNDZfMV8z" +
            ".mp4?tag=1-1484283500-p-0-ulbic0kf0g-b8695a420ceeecd8 HTTP/1.1\" 200 2222772 \"-\" \"kwai-android\" mem 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000 1058 1 - _ 111.79.129.22 - 2222452 59.63.188.183";
        val log7 = "CDNLOG_cache ctl-zj-122-226-182-077 121.196.223.22 3219.055 - [13/Jan/2017:15:27:43 +0800] \"GET " +
            "http://kwmov.a.yximgs.com/upic/2017/01/13/12/BMjAxNzAxMTMxMjI0NDBfMTk1MjQxNjFfMTUwMTM5Nzc0Nl8xXzM=.mp4?tag=1-1484292307-f-0-i40mawzhyn-72016b59a56dea9e HTTP/1.1\" 200 1953709 \"-\" \"kwai-android\" FCACHE_HIT_DISK 0.000 0.000 - - - - 0.000 3219.055 3219.055 1 - - 121.196.223.22 DISK HIT from 122.226.182.77 1953329 122.226.182.77";
        println(SlowRateLogInfo(log7))
        //        println(SlowRateLogInfo(log1))
//        println(SlowRateLogInfo(log2))
//        println(SlowRateLogInfo(log3))
//        println(SlowRateLogInfo(log4))
//        println(SlowRateLogInfo(log5))
//        println(SlowRateLogInfo(log6))
        println(SlowRateLogInfo("") match {
              case Left(msg) => msg
              case Right(result) => null
        })
//        val source = Source.fromFile("F:\\业务资料\\快手慢速比计算\\mm")
//        var i = 0
//        source.getLines().foreach( r => {
//             i = i + 1
//             if(i.equals(50)){
//                 return
//             }
//            println(SlowRateLogInfo(r) match {
//                case Left(msg) => msg
//                case Right(result) => null
//            })
//        })
//        source.close()

    }

    def apply(log: String): Either[SlowRateLogInfo, String] = {
        if (log.startsWith("@|@")) {
            Try(processLog(logTrim(log))) match {
                case Success(result) => Left(result)
                case Failure(e) => Right(log)
            }
        } else {
            Try(processLog(log)) match {
                case Success(result) => Left(result)
                case Failure(e) => Right(log)
            }
        }
    }

    def logTrim(log: String): String = {
        val index1 = log.indexOf("@|@")
        val index2 = log.indexOf("@|@", index1 + 3)
        val index3 = log.indexOf("@|@", index2 + 3)
        val index4 = log.indexOf("@|@", index3 + 3)

        val log1 = log.substring(index4 + 3)

        val index5 = log1.indexOf("@|@")
        val index6 = log1.indexOf("@|@", index5 + 3)
        val log2 = log1.substring(0, index5)
        val log3 = log1.substring(index6 + 3)
        log2 + log3
    }

    /**
      * 过滤掉127.0.0.1的日志，size小于200K，es小于1秒，只计算状态码为2XX的日志
      *
      * @param log
      * @return
      */
    def processLog(log: String): SlowRateLogInfo = {
        val index1 = log.indexOf(" ")
        val index2 = log.indexOf(" ", index1 + 1)
        val host = log.substring(index1 + 1, index2)

        val lastIndex1 = log.lastIndexOf(" ")
        val hostIP = log.substring(lastIndex1 + 1)

        val index3 = log.indexOf(" ", index2 + 1)
        // val index4 = log.indexOf(" ", index3 + 1)
        val userIp = log.substring(index2 + 1, index3)

        if (userIp.equals("127.0.0.1")) {
            return null
        }

        val index4 = log.indexOf(" ", index3 + 1)
        val esStr = log.substring(index3 + 1, index4).trim
        if(esStr.equals("-")){
            return null
        }
        val es = esStr.toFloat

        if (es < timeThreshold * 1000) {
            return null
        }

        val timeIndex1 = log.indexOf("[", index4)
        val timeIndex2 = log.indexOf("]", timeIndex1)
        val timeS = log.substring(timeIndex1 + 1, timeIndex2)
        val time = convertTimeToLong(timeS)

        val domainIndex1 = log.indexOf("://", timeIndex2)
        val domainIndex2 = log.indexOf("/", domainIndex1 + 3)
        val domain = log.substring(domainIndex1 + 3, domainIndex2)

        if (!SlowRateFilterKafkaReceiver.domainsFilter.contains(domain)) {
            return null
        }

        val index6 = log.indexOf("\"", domainIndex2)
        val index7 = log.indexOf(" ", index6 + 2)
        val status = log.substring(index6 + 2, index7)
        if (status.startsWith("4") || status.startsWith("5")) {
            return null
        }

        val csIndex1 = log.indexOf(" ", index7 + 2)
        val cs = log.substring(index7 + 1, csIndex1).trim
        if(cs.equals("-")){
            return null
        }
        if (cs.toFloat < sizeThreshold * 1024) {
            return null
        }
        val speed = cs.toFloat / es * 1000 / 1024; // B/ms *1000/1024?

        val userIpLong = ipToLong(userIp)

        val userNodeIP = IPAreaISP.getNode(userIpLong)
        val userIsp = userNodeIP.isp
        val userPrv = userNodeIP.prvName

        val hostIPLong = ipToLong(hostIP)

        val hostIPNode = IPAreaISP.getNode(hostIPLong)
        val hostIsp = hostIPNode.isp
        val hostPrv = hostIPNode.prvName

        val ifSlow = if (speed < speedThreshold) 1 else 0 // 当速度小于100K，视为慢速

        var ispName = userNodeIP.ispName
        var prv = userPrv
        if (!userIsp.equals(hostIsp)) {
            ispName = "other"
            prv = "other"
        }
      //  println(speed)
        new SlowRateLogInfo(domain, host, ispName, prv, time, ifSlow)
    }

    def convertTimeToLong(time: String): Long = {
        val format = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
        val tmp = format.parse(time).getTime / 1000;
        tmp - tmp % 300 + 300
    }

    def ipToLong(ipString: String): Long = {
        val l = ipString.split("\\.").map((x: String) => x.toLong)
        var sum: Long = 0
        for (e <- l) {
            sum = (sum << 8) + e
        }
        sum
    }
}
