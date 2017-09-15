package com.fastweb.cdnlog_analysis.isms

import scala.io.{BufferedSource, Source}

/**
  * Created by lfq on 2017/4/6.
  */
object IsmsFilter {
    val url = "http://udap.fwlog.cachecn.net:8089/udap/isms-filter.domains"

    val updateInterval = 3600 * 1000 //更新时间间隔，3600秒 ,以毫秒为单位

    var lastUpdateTime = 0l

    var domainSet:Set[String] = Set()

    def getDomains(): Either[Boolean, Set[String]] = {
        var result: Boolean = true
        var source: BufferedSource = null
        var domainSet: Set[String] = null
        try {
            source = Source.fromURL(url)
            source.getLines().foreach(line => {
                domainSet = line.split(",").toSet
            })
        } catch {
            case ex: Exception => {
                result = false
            }
        } finally {
            if (source != null) {
                source.close()
            }
        }
        if (result) Right(domainSet) else Left(result)
    }

    def update(): Unit ={
        val currentTime = System.currentTimeMillis()
        if((currentTime - lastUpdateTime ) > updateInterval ){
            getDomains() match {
                case Right(set) => {
                    domainSet = set
                    lastUpdateTime = currentTime
                }
                case Left(boolean) => null
            }
        }
    }

    def filterDomain(domain: String): Boolean = {
        update()
        if(domainSet.contains(domain)) true else false
    }

    def main(args: Array[String]) {
        println(filterDomain("www.163.com"))
        println(filterDomain("www.163.co"))
    }
}
