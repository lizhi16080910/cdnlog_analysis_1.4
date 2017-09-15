package com.fastweb.cdnlog_analysis.filter

import org.apache.spark.streaming.kafka.filter.Filter


/**
  * Created by lfq on 2016/8/8.
  */
class DomainFilter extends Filter{
    override def filter(line: String): Boolean = {
        val domains = Domains.apply()
        try{
            val firstBlankIndex = line.indexOf(" ") + 1
            val secondBlankIndex = line.indexOf(" ", firstBlankIndex) + 1
            val domainEndIndex = line.indexOf("#_#")

            if (domainEndIndex < 0) {
                return false
            }
            val domain = line.substring(secondBlankIndex , domainEndIndex)
            if(domains.contains(domain)){
                return true
            }
        }catch{
            case e:Exception =>  {
                e.printStackTrace()
                false
            }
        }
        false
    }
}

object DomainFilter {
    def main(args:Array[String]): Unit ={
        val filter = classOf[DomainFilter]
        val fl = filter.newInstance()
        println(fl.filter("cdnlog_edge csjjj-180-89-88-88 img3.imgserver.kuaikuai.cn#_#-#_#"))
    }
}
