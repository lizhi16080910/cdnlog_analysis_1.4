package com.fastweb.cdnlog_analysis.duowan

import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.log4j.Logger
import scala.annotation.tailrec
import scala.io.Source
import com.fastweb.cdnlog_analysis.Channel
import scala.util.Try
import scala.util.Success
import scala.util.Failure

case class DuowanLogInfo(machine: String, ip: Long, isp: String, prv: String, domain: String, url: String, suffix: String, status: String,
                   cs: Long, refer: String, ua: String, es: Double, hit: String, timestmp: String, dd: Int, userid: String, rk: String, 
                   up: String, Fa: String, Fv: String, platform: String, node: String)



object DuowanLogInfo {
  private[this] val suffixSet = Set("jpg", "gif", "png", "bmp", "ico", "swf",
    "css", "js", "htm", "html", "doc", "docx", "pdf", "ppt", "txt", "zip", "rar",
    "gz", "tgz", "exe", "xls", "cvs", "xlsx", "pptx", "mp3", "mp4", "rm", "mpeg",
    "wav", "m3u", "wma", "avi", "mov", "av", "flv", "f4v", "hlv", "f4fmp4", "f4fmp3",
    "ts", "dv", "fhv","xml","apk","vxes","ipa","deb")

  private val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  private val timeFormat = "dd/MMM/yyyy:HH:mm:ss Z"
  private[this] val log = Logger.getLogger(this.getClass)
  
  val ispList = getResource("isp")
  val prvList = getResource("prv")

  def getResource(path: String): Array[Array[Long]] = {
    Source.fromInputStream(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(path)).getLines()
      .map(_.split(" ").map(x => x.toLong)).toArray
  }

  def apply(line: String, channel: Channel): Either[DuowanLogInfo,String] = {
    Try(parseStringduowanLog(line, channel)) match
    {
      case Success(result) => Left(result)
      case Failure(e) => Right(line)
    }
  }
  
//  def apply2(line: String, channel: Channel): Try[DuowanLogInfo] = Try(parseStringduowanLog(line, channel))

  
  

  def parseStringduowanLog(line: String, channel: Channel): DuowanLogInfo = {

    var logline = line

    //判断日志如否带眼睛
    if(line.startsWith("@|@"))
      {
        val Arraytemp = line.split("@\\|@")
        logline = Arraytemp(4)+Arraytemp(6)
      }

    //解析日志字段
    val splitline = logline.split("#_#")
    val machine = splitline(0).split(" ")(1)
    val ip = if(splitline(1).equals("127.0.0.1"))
    {
      throw new RuntimeException("")
    }else{
      ipToLong(splitline(1))
    }   
    val prv = String.valueOf(search(prvList, ip))
    val isp = String.valueOf(search(ispList, ip))
    var domain = "-"
    if((splitline.length==16)&&(splitline(15).startsWith("https"))){
       domain = "https_"+channel.transform(splitline(0).split(" ")(2))
    }else{
      domain = channel.transform(splitline(0).split(" ")(2))
    }
    val url = getURLRelated(splitline(4), channel,domain)(0)
    val suffix = getURLRelated(splitline(4), channel,domain)(1)
    val status = splitline(5);
    val cs = splitline(14).split(" ")(0).toLong
    val refer = splitline(7)
    val ua = getUAType(splitline(8))
    val es = Try(splitline(9).toDouble).getOrElse(0.00)       
    val time = splitline(3)
    val timestmp1 = time.substring(1,time.length()-1)
    val timestmp = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(timestmp1))
    val hit = splitline(12).split(" ")(0)
    val dd = 0
    val userid = getURLRelated(splitline(4), channel,domain)(2)
    val rk = "-"
    val up = "-"
    val Fa = "-"
    val Fv = "-"
    val platform = "-"
    val nodeStrArray = splitline(0).split(" ")(1).split("-")
    val nodeStr = nodeStrArray(2) + "." + nodeStrArray(3) + "." + nodeStrArray(4) + "." + nodeStrArray(5)
    val node = try { String.valueOf(ipToLong(nodeStr)) } catch { case e: Throwable => { "0" } }

    DuowanLogInfo(machine, ip, isp, prv, domain, url, suffix, status,
      cs, refer, ua, es, hit, timestmp, dd, userid, rk,
      up, Fa, Fv, platform, node)
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
    } else if (ua_l.contains("windows nt") || ua_l.contains("linux") || ua_l.contains("msie") || ua_l.contains("firefox") || ua_l.contains("chrome")) {
      "pc"
    } else {
      "other"
    }
  }

  /** get domain, url, suffix, userid    param1: GET http://xxx.com/a/b/c.jpg  **/
  def getURLRelated(method_url_http: String, channel: Channel, domain: String): Array[String] = {
    val urltemp = method_url_http.split(" ")(1)
    val index = urltemp.indexOf("?")
    val url = if (index <0)
    {
      urltemp
    }else{
      urltemp.substring(0,index)
    }    
    val userid = channel.getUserID(domain)
    val dot = url.lastIndexOf('.')
    val suffix = if (dot < 0) {
      "other"
    } else {
      val suffix_o = url.substring(dot + 1)
      if (suffixSet.contains(suffix_o.toLowerCase)) {
        suffix_o.toLowerCase
      } else {
        "other"
      }
    }
    Array(url, suffix, userid)
  }


  def main(args: Array[String]) {
    val channel = Channel.update()
    val log1 = "CDNLOG cnc-jl-175-022-002-182 dl.vip.yy.com#_#139.214.117.15#_#-#_#[22/Aug/2016:16:13:03 +0800]#_#GET /vipface/20140411.7z HTTP/1.1#_#200#_#1148235#_#-#_#-#_#731.111#_#0.000#_#175.22.2.209#_#TCP_HIT#_#fastweb#_#1148591#_#http 175.22.2.182"
    val log2 = "@|@c06.i06@|@68@|@CTL_Taizhou2_74_89@|@CDNLOG_cache ctl-js-061-147-218-018 @|@00000000@|@dl.vip.yy.com#_#61.191.254.226#_#-#_#[09/Nov/2016:08:58:28 +0800]#_#GET http://dl.vip.yy.com/vipskin/icon/32.png HTTP/1.1#_#200#_#8241#_#-#_#-#_#0.000#_#0.000#_#59.63.188.153#_#TCP_HIT-#_#fastweb#_#8667 59.63.188.151"
    for (i <- List(log1,log2)) {
      //      apply(i, channel)  match {
      //        case Left(msg) => println(msg)
      //        case Right(source) => println(source)
      //      }
      val resultlog = parseStringduowanLog(i, channel)
      println(resultlog)
    }

  }
}