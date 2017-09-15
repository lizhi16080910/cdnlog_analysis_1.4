package com.fastweb.cdnlog_analysis

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.elasticsearch.spark.sparkContextFunctions
import org.elasticsearch.spark.sparkRDDFunctions

object CdnlogPnoOfflineApp {
  def main(args: Array[String]) {
    val Array(esNodes, startTime, stopTime, uiPort) = args

    val conf = new SparkConf()
      .setAppName("CdnlogPnoOfflineApp:hour")
      .set("spark.akka.frameSize", "128")
      .set("spark.kryoserializer.buffer.mb", "256")
      //.set("spark.executor.memory", memSize + "g")
      .set("spark.default.parallelism", "30")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.ui.port", uiPort)
      .set("es.index.auto.create", "true")
      .set("es.nodes", esNodes)

    val sc = new SparkContext(conf)

    val computeMinutesEsType = () => {
      val dateNow = new Date(startTime.toLong * 1000)
      val df = new SimpleDateFormat("yyyyMMddHH")
      val esType = df.format(dateNow)
      esType
    }

    val getEsType = () => {
      val date_now = new Date(startTime.toLong * 1000)
      val df = new SimpleDateFormat("yyyyMMdd")
      val esType = df.format(date_now)
      esType
    }

    def getFiveTimestamp = (timestamp: String) => {
      (timestamp.toLong / 300 * 300 + 300).toString()
    }
    val yearTimestamp = 12L * 30 * 24 * 60 * 60
    def filterTimestamp = (timestamp: String) => {
      Math.abs(timestamp.toLong - startTime.toLong) < yearTimestamp
    }

    val pnoCs = "pno_cs/" + computeMinutesEsType()
    val queryString = "" //"{\"filter\":{\"range\":{\"timestamp\":{\"from\":\"" + startTime + "\",\"to\":\"" + stopTime + "\"}}}}"
    /**
     * 查询获取分钟级别的pno流量
     */
    val minutePnoRDD = sc.esRDD(pnoCs, queryString).map(x => x._2)

    /**
     * 每5分钟计算pno的cs，计算规则 01--04 ==》》05  向前求和计算
     */
    var pnoCsFiveMinute = minutePnoRDD.map(x => {
      val domain = x.get("domain").get.toString()
      val pno = x.get("pno").get.toString()
      val timestamp = getFiveTimestamp(x.get("timestamp").get.toString())
      val cs = x.get("cs").get.toString().toLong
      ((domain, pno, timestamp), cs)
    }).filter(x => filterTimestamp(x._1._3))
      .reduceByKey((x1, x2) => (x1 + x2))
      .map(x => {
        Map("domain" -> x._1._1,
          "pno" -> x._1._2,
          "timestamp" -> x._1._3,
          "write_time" ->computeMinutesEsType(),
          "cs" -> x._2)
      })
      .saveToEs("pno_off_five/" + getEsType())

    sc.stop()
  }
}