package com.fastweb.cdnlog_analysis

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.io.Source
import scala.util.parsing.json.JSON


case class JsonLogInfo(hostname:String,ip: Long,isp:String,prv:String,domain:String,ru:String,
                       suffix:String,Fs:String,cs:Long,refer:String, ua:String,es:Double,st:String,timestamp:String,
                       dd:Int,userid:String,tl:Double,rm:String,rv:String,rf:String,a:String,timestampLong:Long)



object JsonLogInfo {
  private[this] val suffixSet = Set("jpg", "gif", "png", "bmp", "ico", "swf",
    "css", "js", "htm", "html", "doc", "docx", "pdf", "ppt", "txt", "zip", "rar",
    "gz", "tgz", "exe", "xls", "cvs", "xlsx", "pptx", "mp3", "mp4", "rm", "mpeg",
    "wav", "m3u", "wma", "avi", "mov", "av", "flv", "f4v", "hlv", "f4fmp4", "f4fmp3",
    "ts", "dv", "fhv","xml","apk","vxes","ipa","deb")

  private val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  private val dateFormatMM = new SimpleDateFormat("yyyyMMddHHmm")

  private[this] val log = Logger.getLogger(this.getClass)
  val format = new SimpleDateFormat("yyyyMMddHHmmss")
  val ispList = getResource("isp")
  val prvList = getResource("prv")

  def getResource(path: String): Array[Array[Long]] = {
    Source.fromInputStream(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(path)).getLines()
      .map(_.split(" ").map(x => x.toLong)).toArray
  }

  def apply(line: String, channel: Channel): JsonLogInfo = {
    try {
      parseStringJsonLog(line, channel)

    } catch {
      case e: Throwable => {
        log.info("invalid log: " + line)
        log.debug(e.printStackTrace())
        return null
      }
    }
  }

  def errorLog(line:String):Boolean = {
    val flag = line.contains("_jsonparsefailure") match {
      case true =>
        log.info("invalid log: " + line)
        true
      case false => false
    }
    flag
  }


  def main(args: Array[String]) {
    var channel = Channel.update()
    var log1 = """{"es":46.194,"tl":{"value":8.047,"representation":"s"},"rm":"","Fs":200,"ori":"222.161.203.162 46.194 - [12/Jan/2016:14:47:55 +0800] \"GET http://trackercdn.kugou.com/i/?cmd=4&hash=89f9f050f34218d694b702ad3b8979c6&key=f989ae231ad1b5f6c327048245761f5f&pid=1 HTTP/1.1\" 200 590 \"-\" \"KUGOU2012-7751-CLOUD\" FCACHE_MISS  8.047 0.000 0.000 0.000 46.094 0.100 0.100 0.000 46.194\n","st":"FCACHE_MISS","hostname":"cnc-he-060-008-151-222","ru":"http://trackercdn.kugou.com/i/?cmd=4&hash=89f9f050f34218d694b702ad3b8979c6&key=f989ae231ad1b5f6c327048245761f5f&pid=1","@timestamp":"2016-01-12T06:47:55.000Z","ua":"KUGOU2012-7751-CLOUD","rv":"HTTP/1.1","rf":"-","a":"222.161.203.162","cs":590,"lt":1.452581275E18,"@version":"1","type":"cdnlog"}"""
    var log2 = """{"es":,"tl":{"value":8.047,"representation":"s"},"rm":"GET","Fs":200,"ori":"222.161.203.162 46.194 - [12/Jan/2016:14:47:55 +0800] \"GET http://trackercdn.kugou.com/i/?cmd=4&hash=89f9f050f34218d694b702ad3b8979c6&key=f989ae231ad1b5f6c327048245761f5f&pid=1 HTTP/1.1\" 200 590 \"-\" \"KUGOU2012-7751-CLOUD\" FCACHE_MISS  8.047 0.000 0.000 0.000 46.094 0.100 0.100 0.000 46.194\n","st":"FCACHE_MISS","hostname":"cnc-he-060-008-151-222","ru":"http://trackercdn.kugou.com/i/?cmd=4&hash=89f9f050f34218d694b702ad3b8979c6&key=f989ae231ad1b5f6c327048245761f5f&pid=1","@timestamp":"2016-01-12T06:47:55.000Z","ua":"KUGOU2012-7751-CLOUD","rv":"HTTP/1.1","rf":"-","a":"222.161.203.162","cs":590,"lt":1.452581275E18,"@version":"1","type":"cdnlog"}"""
    var log3 = """{"test":00,"es":46.194"tl":{"value":8.047,"representation":"s"},"rm":"GET","Fs":200,"ori":"222.161.203.162 46.194 - [12/Jan/2016:14:47:55 +0800] \"GET http://trackercdn.kugou.com/i/?cmd=4&hash=89f9f050f34218d694b702ad3b8979c6&key=f989ae231ad1b5f6c327048245761f5f&pid=1 HTTP/1.1\" 200 590 \"-\" \"KUGOU2012-7751-CLOUD\" FCACHE_MISS  8.047 0.000 0.000 0.000 46.094 0.100 0.100 0.000 46.194\n","st":"FCACHE_MISS","hostname":"cnc-he-060-008-151-222","ru":"http://trackercdn.kugou.com/i/?cmd=4&hash=89f9f050f34218d694b702ad3b8979c6&key=f989ae231ad1b5f6c327048245761f5f&pid=1","@timestamp":"2016-01-12T06:47:55.000Z","ua":"KUGOU2012-7751-CLOUD","rv":"HTTP/1.1","rf":"-","a":"222.161.203.162","cs":590,"lt":1.452581275E18,"@version":"1","type":"cdnlog"}"""

    val log4 = """{"es":0.11,"tl":{"value":0.954,"representation":"s"},"rm":"GET","Fs":200,"ori":"121.17.105.103 71.110 - [12/Jan/2016:20:20:32 +0800] \"GET http://fragment.firefoxchina.cn/res/img/default.png HTTP/1.1\" 200 2256 \"http://fragment.firefoxchina.cn/res/css/main-min.css?v=20150604\" \"Mozilla/5.0 (Windows NT 6.1; rv:43.0) Gecko/20100101 Firefox/43.0\" FCACHE_IMS_HIT  0.954 0.000 0.000 0.000 71.016 - 0.094 0.000 71.110\n","st":"FCACHE_IMS_HIT","hostname":"cnc-he-060-008-151-222","ru":"http://fragment.firefoxchina.cn/res/img/default.png","@timestamp":"2016-01-12T12:20:32.000Z","ua":"Mozilla/5.0 (Windows NT 6.1; rv:43.0) Gecko/20100101 Firefox/43.0","rv":"HTTP/1.1","rf":"http://fragment.firefoxchina.cn/res/css/main-min.css?v=20150604","a":"121.17.105.103","cs":2256,"lt":1.452601232E18,"@version":"1","type":"cdnlog"}"""

    val log5 = """{"es":0,"tl":{"value":58.941,"representation":"s"},"rm":"GET","Fs":200,"ori":"101.26.83.145 0.000 - [12/Jan/2016:21:53:39 +0800] \"GET http://p2.pstatp.com/list/c00055955,ec06cbc3 HTTP/1.1\" 200 4609 \"-\" \"okhttp/2.4.0\" FCACHE_HIT_MEM  58.941 0.000 - - - - 0.000 0.000 0.000\n","st":"FCACHE_HIT_MEM","hostname":"cnc-he-060-008-151-222","ru":"http://p2.pstatp.com/list/c00055955,ec06cbc3","@timestamp":"2016-01-12T13:53:39.000Z","ua":"okhttp/2.4.0","rv":"HTTP/1.1","rf":"-","a":"101.26.83.145","cs":4609,"lt":1.452606819E18,"@version":"1","type":"cdnlog"}"""

    val log6 = """{"es":0.075,"tl":{"value":0.986,"representation":"s"},"rm":"GET","Fs":200,"ori":"121.21.146.144 0.075 - [13/Jan/2016:09:07:36 +0800] \"GET http://p2.pstatp.com/video1609/c000acb8526c5f153 HTTP/1.1\" 200 11783 \"-\" \"okhttp/2.4.0\" FCACHE_HIT_MEM  0.986 0.000 - - - - 0.075 0.000 0.075\n","st":"FCACHE_HIT_MEM","hostname":"cnc-he-060-008-151-222","ru":"http://p2.pstatp.com/video1609/c000acb8526c5f153","@timestamp":"2016-01-13T01:07:36.000Z","ua":"okhttp/2.4.0","rv":"HTTP/1.1","rf":"-","a":"121.21.146.144","cs":11783,"lt":1.452647256E18,"@version":"1","type":"cdnlog"}"""

    val log7 = """{"es":0,"tl":{"value":12.036,"representation":"s"},"rm":"GET","Fs":200,"ori":"42.59.42.74 0.000 - [13/Jan/2016:14:57:57 +0800] \"GET http://cu005.www.duba.net/duba/2010/bin/1335/kav/data/rcmdv2sp01/cfgdefault/pic/9b92340faf19e0be9beddf3e1fa38832 HTTP/1.1\" 200 3403 \"-\" \"-\" FCACHE_HIT_MEM 12.036 0.000 - - - - 0.093 0.000 0.093\n","st":"FCACHE_HIT_MEM","hostname":"cnc-ln-061-137-188-200","ru":"http://cu005.www.duba.net/duba/2010/bin/1335/kav/data/rcmdv2sp01/cfgdefault/pic/9b92340faf19e0be9beddf3e1fa38832","@timestamp":"2016-01-13T06:57:57.000Z","ua":"-","rv":"HTTP/1.1","rf":"-","a":"42.59.42.74","cs":3403,"lt":1.452668277E18,"@version":"1","type":"cdnlog"}  """

    val log8 ="""{"es":0,"tl":{"value":259.761,"representation":"s"},"rm":"GET","Fs":200,"ori":"180.76.139.243 0.000 - [13/Jan/2016:17:45:39 +0800] \"GET http://i8.qhimg.com/t019fe0a284a9b11ef1.jpg HTTP/1.1\" 200 2445 \"http://gouwu.360.cn/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko\" FCACHE_HIT_MEM  259.761 0.000 - - - - 0.000 0.000 0.000\n","st":"FCACHE_HIT_MEM","hostname":"cnc-tj-125-039-005-010","ru":"http://i8.qhimg.com/t019fe0a284a9b11ef1.jpg","@timestamp":"2016-01-13T09:45:39.000Z","ua":"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko","rv":"HTTP/1.1","rf":"http://gouwu.360.cn/","a":"180.76.139.243","cs":2445,"lt":1.452678339E18,"@version":"1","type":"cdnlog"}"""


    val log9 = """{"es":0.1,"tl":{"value":2.56,"representation":"s"},"rm":"GET","Fs":200,"ori":"106.8.187.130 0.100 - [15/Jan/2016:10:43:21 +0800] \"GET http://update.leak.360.cn/config/config.dat HTTP/1.1\" 200 3292 \"-\" \"-\" FCACHE_HIT_MEM  2.560 0.000 - - - - 0.100 0.000 0.100\n","st":"FCACHE_HIT_MEM","hostname":"ctl-ha-001-193-188-029","ru":"http://update.leak.360.cn/config/config.dat","@timestamp":"2016-01-15T02:43:21.000Z","ua":"-","rv":"HTTP/1.1","rf":"-","a":"106.8.187.130","cs":3292,"lt":1.452825801E18,"@version":"1","type":"cdnlog"}"""

    val log10 = """ {"a":"117.140.224.165 _jsonparsefailure","Fs":200,"ct":4146.514,"ru":"http://p2.qhimg.com/t018f8d0e0a8226d482.png","@timestamp":"2016-01-15T03:13:15.000Z","rv":"HTTP/1.1","rp":0,"lt":1.452827595E18,"tr":0,"rm":"GET","ori":"117.140.224.165 0.000 - [15/Jan/2016:11:13:15 +0800] \"GET http://p2.qhimg.com/t018f8d0e0a8226d482.png HTTP/1.1\" 200 630 \"http://www.ludashi.com/cms/pc/newsmini_v2/live.html?tag=meinv\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0)\" FCACHE_HIT_MEM  4146.514 0.000 - - - - 0.000 0.000 0.000\n","st":"FCACHE_HIT_MEM","hostname":"cmb-gx-111-012-014-142","tc":0,"ua":"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0)","es":0,"cs":630,"rf":"http://www.ludashi.com/cms/pc/newsmini_v2/live.html?tag=meinv","@version":"1","type":"cdnlog"}"""

   for (i <- List(log1,log4,log5,log6,log7,log9)){
    val resultlog = parseJsonLog(i, channel)
      println(resultlog)
    }
   val resultlog = parseStringJsonLog(log10, channel)
    println(resultlog)


    /*

        println(ipToLong("157.55.39.149"))

        val resultlog = parseStringJsonLog(log9, channel).tl
        println(resultlog)
         println(String.valueOf(search(ispList, 2637637525L)))
         */






  }

  def parseStringJsonLog(line: String, channel: Channel):JsonLogInfo = {
    if (errorLog(line)){
      return  null
    }

    def parse(begin: String, end: String): String = {
      if(!line.contains(begin)){
        return ""
      }
      line.substring(line.indexOf(begin, 0) + begin.length(), line.indexOf(end, line.indexOf(begin, 0) + begin.length()))
    }

    var tl = 0D
    var tl_t =parse("\"value\":", ",")
    if (!tl_t.equals("")){
      tl = tl_t.toDouble
    }

    val ru = parse("\"ru\":\"", "\",\"")
    val domain = parse("\"ru\":\"http://", "/")
    val domain_t = channel.transform(domain)

    val rm = parse("\"rm\":\"", "\",\"")
    val Fs = parse("\"Fs\":", ",")
    val st = parse("\"st\":\"", "\",\"")
    val ua = getUAType(parse("\"ua\":\"", "\",\""))

    var es = 0D
    var es_t = parse("\"es\":", ",")
    if(!es_t.equals("")){
        es = es_t.toDouble
      }

    val rv = parse("\"rv\":\"", "\",\"")
    val rf = parse("\"rf\":\"", "\",\"")
    val a = parse("\"a\":\"", "\",\"")
    var 	userid = ""
    if(domain_t != null){
      userid = channel.getUserID(domain_t)
    }else{
      return null
    }
    val hostname = parse("\"hostname\":\"", "\",\"")
    val ip = ipToLong(parse("\"a\":\"", "\",\""))

    val etime = parse("\"lt\":", "E").replace(".", "")
    var time = etime
    if(etime.length() < 10){
      val j = 10-etime.length()
      for(i <- 1 to j){
        time = time + "0"
      }
    }
    val timestampLong = time.toLong
    val timestamp = timeLongToStr(timestampLong)

    val prv = String.valueOf(search(prvList, ip))

    val isp = String.valueOf(search(ispList, ip))

    var cs = 0L
    val cs_t = parse("\"cs\":", ",\"")
    if(!cs_t.equals("")){
      cs = cs_t.toLong
    }

    var  suffix= ru.substring(ru.lastIndexOf(".")+1)
    if (!suffixSet.contains(suffix)){
      suffix = "other"
    }

    val refer = ""
    val dd = 0


    JsonLogInfo(hostname,ip,isp,prv,domain_t,ru,suffix,Fs,cs,refer,ua,es,st,timestamp,dd,userid,tl,rm,rv,rf,a,timestampLong)

  }


  def parseJsonLog(line: String, channel: Channel):JsonLogInfo = {

    val result = JSON.parseFull(line)

    result match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, Any]) => {
        val es = map.get("es").get.asInstanceOf[Double]
        val tl = map.get("tl").get.asInstanceOf[Map[Any, Any]].get("value").get.asInstanceOf[Double]
        val rm = map.get("rm").get.asInstanceOf[String]
        val Fs = map.get("Fs").get.asInstanceOf[Double].toLong.toString
        //val ori = map.get("ori").get.asInstanceOf[String]
        val st = map.get("st").get.asInstanceOf[String]
        val hostname = map.get("hostname").get.asInstanceOf[String]
        //val timestamp = map.get("@timestamp").get.asInstanceOf[String]
        val lt = map.get("lt").get.asInstanceOf[Double]
        val timestamp = lt.toLong/1000/1000/1000
        val ru = map.get("ru").get.asInstanceOf[String]

        val domain = channel.transform(parse(ru,"http://", "/"))
        var 	userid = ""
        if(domain != null){
          userid = channel.getUserID(domain)
        }else{
          return null
        }

        val ua = getUAType(map.get("ua").get.asInstanceOf[String])
        val rv = map.get("rv").get.asInstanceOf[String]
        val rf = map.get("rf").get.asInstanceOf[String]
        val a = map.get("a").get.asInstanceOf[String]
        val cs = map.get("cs").get.asInstanceOf[Double]


        val ip = ipToLong(a)
        val isp = search(ispList, ip).toString
        val prv = search(prvList, ip).toString

        var  suffix= ru.substring(ru.lastIndexOf(".")+1)
        if (!suffixSet.contains(suffix)){
          suffix = "other"
        }
        val refer = ""
        val dd = 0


        JsonLogInfo(hostname,ip,isp,prv,domain,ru,suffix,Fs,cs.toLong,refer,ua,es,st,timeLongToStr(timestamp),dd,userid,tl,rm,rv,rf,a,timestamp)
      }
      case None => null
      case other => null
    }
  }

  def timeToLong(time:String):Long ={
    dateFormat.parse(time).getTime/1000
  }

  def timeLongToStr(time:Long):String = {
    val date = new Date(time*1000L)
    dateFormat.format(date)
  }


  def parse(line:String,begin: String, end: String): String = {
    line.substring(line.indexOf(begin, 0) + begin.length(), line.indexOf(end, line.indexOf(begin, 0) + begin.length()))
  }

  def search(list: Array[Array[Long]], ip: Long): Long = {
    def compare(ip: Long, line: Array[Long]): Int = {
      if (ip < line(0)) {
        -1
      } else if (ip > line(1)) {
        1
      } else {
        0
      }
    }


    @tailrec
    def searchIn(low: Int, high: Int): Long = {
      val m = (low + high) / 2
      val state = compare(ip, list(m))
      if (state == 0) {
        try {
          list(m)(2)
        } catch {
          case e: Throwable => {
            0
          }
        }
      } else if (high <= low) {
        0
      } else if (state < 0) {
        searchIn(low, m - 1)
      } else {
        searchIn(m + 1, high)
      }
    }
    searchIn(0, list.length - 1)
  }

  def ipToLong(ipString: String): Long = {
    val l = ipString.split("\\.").map((x: String) => x.toLong)
    var sum: Long = 0
    for (e <- l) {
      sum = (sum << 8) + e
    }
    sum
  }

  def getUAType(uaString: String): String = {
    val ua_l = uaString.toLowerCase()
    if (ua_l.contains("inphic") || ua_l.contains("miui") || ua_l.contains("letv") || ua_l.contains("diyomate")) {
      "tv"
    } else if (ua_l.contains("android") || ua_l.contains("iphone") || ua_l.contains("ipad")) {
      "mobile"
    } else if (ua_l.contains("windows nt") || ua_l.contains("linux") || ua_l.contains("msie") ||  ua_l.contains("firefox") ||  ua_l.contains("chrome") ) {
      "pc"
    } else {
      "other"
    }
  }


}