package com.fastweb.cdnlog_analysis

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import org.apache.log4j.Logger
import scala.annotation.tailrec
import scala.io.Source


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

  def apply(line: String, channel: Channel): DuowanLogInfo = {
    try {
      parseStringduowanLog(line, channel)

    } catch {
      case e: Throwable => {
        log.info("invalid log: " + line)
        log.debug(e.printStackTrace())
        return null
      }
    }
  }

  def main(args: Array[String]) {
    var channel = Channel.update()
    var log1 = "CDNLOG_cache cmb-sx-183-203-008-133 huyaimg.dwstatic.com#_#111.19.80.167#_#-#_#[22/Dec/2016:16:52:57 +0800]#_#GET http://huyaimg.dwstatic.com/avatar/1084/b7/896bc815db9560eabbcb4a227f62ba_180_135.jpg?1416387967 HTTP/1.1#_#200#_#4239#_#-#_#-#_#0.000#_#0.000#_#183.203.8.130#_#TCP_HIT -#_#fastweb#_#4665 183.203.8.133"
    var log2 = "CDNLOG_cache cmb-sx-183-203-008-133 m1.dwstatic.com#_#117.136.97.40#_#-#_#[22/Dec/2016:16:52:57 +0800]#_#GET http://m1.dwstatic.com/picmbox/mboxMomPic/01/201612/022/300_300_x0a15282517695b58bc8fbdb4beb40000.jpg HTTP/1.1#_#200#_#12428#_#-#_#lolbox/212 CFNetwork/808.1.4 Darwin/16.1.0#_#0.000#_#0.000#_#183.203.8.130#_#TCP_HIT -#_#fastweb#_#12826 183.203.8.133"
    var log3 = "CDNLOG_cache cmb-sx-183-203-008-133 huyaimg.dwstatic.com#_#111.53.154.111#_#-#_#[22/Dec/2016:16:52:57 +0800]#_#GET http://huyaimg.dwstatic.com/cdnimage/actprop/20237_0_45_1473822048.jpg?122e026e139adc601a5acad59df94949 HTTP/1.1#_#304#_#0#_#http://hyplayer.dwstatic.com/v3.1_122202/scene/defaultScene.swf#_#Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)#_#0.000#_#0.000#_#183.203.8.130#_#TCP_MISS -#_#fastweb#_#451 183.203.8.133"
    var log4 = "CDNLOG_cache cmb-sx-183-203-008-133 huyaimg.dwstatic.com#_#120.203.220.242#_#-#_#[22/Dec/2016:16:52:58 +0800]#_#GET http://huyaimg.dwstatic.com/avatar/1011/b8/af2730eebd8f4f571fb4491339ec8a_180_135.jpg?1417066308 HTTP/1.1#_#200#_#11247#_#-#_#-#_#0.000#_#0.000#_#183.203.8.130#_#TCP_HIT -#_#fastweb#_#11674 183.203.8.133"
    var log5 = "CDNLOG_cache cmb-sx-183-203-008-133 huyaimg.dwstatic.com#_#111.53.154.111#_#-#_#[22/Dec/2016:16:52:58 +0800]#_#GET http://huyaimg.dwstatic.com/cdnimage/actprop/20245_0_45_1475230263.jpg?28175ab1daba24ca3bd0eec42799666a HTTP/1.1#_#304#_#0#_#http://hyplayer.dwstatic.com/v3.1_122202/scene/defaultScene.swf#_#Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)#_#0.000#_#0.000#_#183.203.8.130#_#TCP_MISS -#_#fastweb#_#445 183.203.8.133"
    for (i <- List(log1,log2,log3,log4,log5)) {
      val resultlog = parseStringduowanLog(i, channel)
      println(resultlog)
    }
  }

  def parseStringduowanLog(line: String, channel: Channel):DuowanLogInfo = {
    
    val splitline = line.split("#_#")
    val machine = splitline(0).split(" ")(1)
    val ip = ipToLong(splitline(1))
    val prv = String.valueOf(search(prvList, ip))
    val isp = String.valueOf(search(ispList, ip))
    val domain = getURLRelated(splitline(4), channel)(0)
    val url = getURLRelated(splitline(4), channel)(1)
    val suffix = getURLRelated(splitline(4), channel)(2)
    val status = splitline(5);
    val cs = splitline(14).split(" ")(0).toLong
    val refer = splitline(7)
    val ua = getUAType(splitline(8))
    val es = splitline(9).toDouble
    val time = splitline(3)
    val timestmp1 = time.substring(1,time.length()-1)
    val timestmp = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(timestmp1))
    val hit = splitline(12).split(" ")(0)
    val dd = 0
    val userid = getURLRelated(splitline(4), channel)(3)
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
    } else if (ua_l.contains("windows nt") || ua_l.contains("linux") || ua_l.contains("msie") ||  ua_l.contains("firefox") ||  ua_l.contains("chrome") ) {
      "pc"
    } else {
      "other"
    }
  }
  
    /** get domain, url, suffix, userid    param1: GET http://xxx.com/a/b/c.jpg  **/
  def getURLRelated(method_url_http: String, channel: Channel): Array[String] = {
    val url_o = method_url_http.split(" ")(1)
    val s_end = url_o.length
    val begin = url_o.indexOf("://") + 3
    var end = url_o.indexOf('/', begin)
    if (end < 0) end = s_end
    var domain = url_o.substring(begin, end)
    domain = channel.transform(domain)
    if (domain == null) throw new RuntimeException("domain is null")
    val userid = channel.getUserID(domain)
    end = url_o.indexOf('?', begin)
    if (end < 0) end = s_end
    val url = url_o.substring(begin, end)
    val dot = url.lastIndexOf('.')
    end = url.length - 1
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
    Array(domain, url, suffix, userid)
  }
}