package com.fastweb.cdnlog_analysis
import org.json4s._
import java.io.InputStream
import java.net.URL
class DirCsDomainConf {

}

object DirCsDomainConf {
  var DIR_CS_DOMAIN_CONF = "http://192.168.100.204:8088/dircs_domain.conf"
  val timeinterval = 1*60*60*1000L

  var lastUpdate = System.currentTimeMillis()

  var current = this(DIR_CS_DOMAIN_CONF)

  def apply(ins: InputStream): Set[String] = {
    val schema_desc = "schema_desc"
    val domain_schema = "domain_schema"
    var value = jackson.parseJson(ins)
    var result = value.children(2).values.asInstanceOf[Map[String, Any]]
    result.keySet.filter { x => !x.equalsIgnoreCase(schema_desc) && !x.equalsIgnoreCase(domain_schema) }
  }

  def apply(): Set[String] = {
    val ins = Thread.currentThread().getContextClassLoader.getResourceAsStream("dircs_domain.conf")
    this(ins)
  }

  def apply(path: String): Set[String] = {
    var url = new URL(path)
    try {
      current = this(url.openStream())
      current
    } catch {
      case e: Throwable => {
        current
      }
    }
  }
  def update(): Set[String] = {
    val currentTime = System.currentTimeMillis()
    if ((currentTime - lastUpdate) > timeinterval) {
      lastUpdate = currentTime
      println("get dircs domain from: " + DIR_CS_DOMAIN_CONF)
      apply(DIR_CS_DOMAIN_CONF)
    } else {
      current
    }
  }
  def main(args: Array[String]) = {
//    println(DirCsDomainConf.apply())
    println(DirCsDomainConf.update().contains("kw11.videocc.net"))
  }
}