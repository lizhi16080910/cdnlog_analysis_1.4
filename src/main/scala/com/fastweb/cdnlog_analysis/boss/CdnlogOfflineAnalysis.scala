package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
 * Created by root on 15-6-3.
 */

object CdnlogOfflineAnalysis {

  def main(args: Array[String]) {
    val Array(mem_size, num_core, num_node, es_node,
      start_time, stop_time, task_type) = args

    val conf = new SparkConf()
      .setAppName("CdnlogOfflineAnalysis:" + task_type)
      .set("spark.akka.frameSize", "128")
      .set("spark.kryoserializer.buffer.mb", "256")
      .set("spark.executor.memory", mem_size + "g")
      .set("spark.default.parallelism", num_core)
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      ///.set("spark.scheduler.allocation.file", pool_path)
      .set("es.index.auto.create", "true")
      .set("es.nodes", es_node)

    val sc = new SparkContext(conf)

    val compute_minutes_es_type = () => {
      val date_now = new Date(start_time.toLong * 1000)
      val df = new SimpleDateFormat("yyyyMMddHH")
      val es_type1 = df.format(date_now)
      val date_pre = new Date(stop_time.toLong * 1000 + 1200*1000)
      val es_type2 = df.format(date_pre)
      es_type1 + "," + es_type2
    }

    val getEsType = () => {
      val date_now = new Date(start_time.toLong * 1000)
      val df = new SimpleDateFormat("yyyyMMdd")
      val es_type = df.format(date_now)
      es_type
    }

    val computeEsIndexAndType = (prefix: String) => {
      if (task_type.equals("hour")) {
        prefix + task_type + "/" + getEsType()
      } else {
        prefix + task_type + "." + getEsType() + "/01"
      }
    }

    val getTimestamp = () => if (task_type.equals("five")) {
      stop_time.toLong / 300 * 300
    } else if (task_type.equals("hour")) {
      start_time.toLong / 3600 * 3600
    } else 0L

    val sourceMachineDomainIndex = "cdnlog.flow.size.count/" + compute_minutes_es_type()
    val queryString = "{\"filter\":{\"range\":{\"timestamp\":{\"from\":\"" + start_time + "\",\"to\":\"" + stop_time + "\"}}}}"
    val sourceMachineDomainRDD = sc.esRDD(sourceMachineDomainIndex, queryString).map(x => x._2)
    val domainIspPrvHitComputeTask = sourceMachineDomainRDD.map(minuteSourceLog => {
      val domain = minuteSourceLog.get("domain").get.toString()
      val isp = minuteSourceLog.get("isp").get.toString()
      val city = minuteSourceLog.get("city").get.toString()
      val prv = minuteSourceLog.get("prv").get.toString()
      val country = minuteSourceLog.get("country").get.toString()
      val hitType = minuteSourceLog.get("hit_type").get.toString()
      val userid = minuteSourceLog.get("userid").get.toString()
      val speedSize = minuteSourceLog.get("speed_size").get.toString().toLong
      val flowSize = minuteSourceLog.get("flow_size").get.toString().toLong
      val reqCount = minuteSourceLog.get("req_count").get.toString().toLong
      val zeroSpeedReqNum = minuteSourceLog.get("zeroSpeedReqNum").get.toString.toLong

      val key = (domain, isp, city, prv, country, hitType, userid)
      val value = (speedSize, flowSize, reqCount, zeroSpeedReqNum)
      (key, value)
    }).reduceByKey((x1: (Long, Long, Long, Long), x2: (Long, Long, Long, Long)) =>
      (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4))
      .map(result => {
      Map("domain" -> result._1._1,
        "isp" -> result._1._2,
        "city" -> result._1._3,
        "prv" -> result._1._4,
        "country" -> result._1._5,
        "timestamp" -> getTimestamp().toString(),
        "hit_type" -> result._1._6,
        "userid" -> result._1._7,
        "speed_size" -> result._2._1,
        "flow_size" -> result._2._2,
        "req_count" -> result._2._3,
        "zeroSpeedReqNum" -> result._2._4)
    }).saveToEs(computeEsIndexAndType("cdnlog.flow.size.count."))

    sc.stop()
  }
}