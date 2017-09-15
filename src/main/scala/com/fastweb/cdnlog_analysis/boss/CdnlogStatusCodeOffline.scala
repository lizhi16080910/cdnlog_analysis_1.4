package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
 * Created by root on 15-6-3.
 */

object CdnlogStatusCodeOffline {

  def main(args: Array[String]) {
    val Array(mem_size, num_core, num_node, es_node,
    start_time, stop_time, task_type) = args

    val conf = new SparkConf()
      .setAppName("CdnlogStatusCodeOffline:" + task_type)
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.mb", "256")
      .set("spark.executor.memory", mem_size + "g")
      .set("spark.default.parallelism", num_core)
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      //////.set("spark.scheduler.allocation.file", pool_path)
      .set("es.index.auto.create", "true")
      .set("es.nodes", es_node)

    val sc = new SparkContext(conf)

    val compute_minutes_es_type = () => {
      
      val date_now = new Date(start_time.toLong * 1000)
      val df = new SimpleDateFormat("yyyyMMddHH")
      val es_type1 = df.format(date_now)
      val date_pre = new Date(stop_time.toLong * 1000 + 1200*1000)
      val es_type2 = df.format(date_pre)
      es_type1+","+es_type2
    }

    val getEsType = () => {
      val date_now = new Date(start_time.toLong * 1000)
      val df = new SimpleDateFormat("yyyyMMdd")
      val es_type = df.format(date_now)
      es_type
    }

    val getTimestamp = () => if (task_type.equals("five")) {
      stop_time.toLong / 300 * 300
    } else if (task_type.equals("hour")) {
      start_time.toLong / 3600 * 3600
    } else 0L

    val computeEsIndexAndType = (prefix: String) => {
      if (task_type.equals("hour")) {
        prefix + task_type + "/" + getEsType()
      } else {
        prefix + task_type + "." + getEsType() + "/01"
      }
    }

    val sourceMachineDomainIndex = "cdnlog.status.code/" + compute_minutes_es_type()
    val queryString = "{\"filter\":{\"range\":{\"timestamp\":{\"from\":\"" + start_time + "\",\"to\":\"" + stop_time + "\"}}}}"
    val sourceMachineDomainRDD = sc.esRDD(sourceMachineDomainIndex, queryString).map(x => x._2)

    var result = sourceMachineDomainRDD.map(x => {
      val domain = x.get("domain").get.toString()
      val isp = x.get("isp").get.toString()
      val city = x.get("city").get.toString()
      val prv = x.get("prv").get.toString()
      val country = x.get("country").get.toString()
      val statusCode = x.get("status_code").get.toString()
      val userid = x.get("userid").get.toString()
      val flowSize = x.get("flow_size").get.toString().toLong
      val reqCount = x.get("req_count").get.toString().toLong
      ((domain, isp, city, prv, country, statusCode, userid), (flowSize, reqCount))
    }).reduceByKey((x1: (Long, Long), x2: (Long, Long)) => (x1._1 + x2._1, x1._2 + x2._2))
      .map(x => {
      Map("domain" -> x._1._1,
        "isp" -> x._1._2,
        "city" -> x._1._3,
        "prv" -> x._1._4,
        "country" -> x._1._5,
        "timestamp" -> getTimestamp().toString(),
        "status_code" -> x._1._6,
        "userid" -> x._1._7,
        "flow_size" -> x._2._1,
        "req_count" -> x._2._2)
    }).saveToEs(computeEsIndexAndType("cdnlog.status.code."))


    sc.stop()
  }
}