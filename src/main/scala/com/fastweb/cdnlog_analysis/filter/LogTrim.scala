package com.fastweb.cdnlog_analysis.filter

import scala.io.Source

/**
  * Created by lfq on 2016/8/19.
  */
object LogTrim {
    def apply(line: String): String = {
        try {
            val bracketIndex = line.indexOf("]#_#")
            if(bracketIndex < 0){
                return ""
            }
            val index2 = line.indexOf("#_#", bracketIndex + 4)
            val request = line.substring(bracketIndex + 4, index2 )

            val index = request.indexOf("://")
            if (index < 0) {
                return line
            }
            val index3 = request.indexOf("/", index + 3)
            val blankIndex = request.indexOf(" ")
            val strR = request.substring(blankIndex + 1, index3)
            val str2 = request.replaceFirst(strR, "")

            val str1 = line.substring(0, bracketIndex + 4)
            val str3 = line.substring(index2)

            str1 + str2 + str3
        } catch {
            case ex: Throwable => {
                ex.printStackTrace()
               // println(line)
                ""
            }
        }
    }

    def main(args: Array[String]) {
        val source = Source.fromFile("F:\\test\\test.data")
        //source.getLines().foreach(line => println(LogTrim(line)))
        source.close()
        val str="CDNLOG_super cnc-js-122-192-111-135 w5.dwstatic.com#_#127.0.0.1#_#-#_#[05/Sep/2016:09:55:01 +0800]#_#GET http://w5.dwstatic.com/do_not_delete/detect.jpg HTTP/1.1#_#200#_#14042#_#-#_#curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.13.1.0 zlib/1.2.3 libidn/1.18 libssh2/1.2.2#_#0.001#_#0.001#_#127.0.0.1#_#TCP_HIT -#_#cdn_agent_fastweb#_#14532 122.192.111.135"
        val str1="CDNLOG_super cnc-js-122-192-111-135 aipai.w5.dwstatic.com#_#124.112.49.228#_#-#_#[05/Sep/2016:09:55:39 +0800]#_#GET /51/3/1633/3032745-137-1471630857.mp4 HTTP/1.1#_#206#_#3850722#_#-#_#Mozilla/5.0 (Linux; Android 5.1.1; Mi-4c Build/LMY47V; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/46.0.2490.85 Mobile Safari/537.36#_#0.328#_#0.328#_#61.130.28.174#_#TCP_HIT -#_#cdn_agent_fastweb#_#3851404 122.192.111.135"
        val str3 = "CDNLOG_super cnc-ha-061-054-031-051 w5.dwstatic.com#_#111.164.57.158#_#-#_#[05/Sep/2016:09:57:42 +0800#_#GET /44/6/1545/183539-100-1446698174.mp4 HTTP/1.1#_#200#_#29289307#_#http://v.huya.com/play/183539.html#_#Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36#_#1.580#_#1.514#_#61.130.28.173#_#TCP_HIT -#_#cdn_agent_fastweb#_#29289844 61.54.31.51"
        val str4="CDNLOG_parent cnc-js-122-192-111-147 117.169.16.195 0.677 67.066 [05/Sep/2016:09:59:47 +0800] \"GET http://m1.dwstatic.com/picmbox/mboxMomPic/01/20157/001/120_120_x0a1528254deb925505b6286629660000.jpg HTTP/1.1\" 200 3382 \"-\" \"lolbox/202 CFNetwork/711.1.16 Darwin/14.0.0\" FCACHE_MISS 0.000 0.000 0.000 0.000 67.066 0.677 67.066 0.677 67.743 1 - 14.17.106.10 117.136.83.77 \"MISS from 122.192.111.147\" 3046 122.192.111.147"
        println(LogTrim(str4))
    }
}
