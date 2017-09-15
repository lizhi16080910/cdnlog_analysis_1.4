/**
 * Created by zery on 15-8-20.
 */
package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat
import java.util.Date
import org.elasticsearch.spark._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object CdnlogMachineOffline {


  def main(args: Array[String]) {
    val Array(es_node,start_time, stop_time, task_type) = args

    val conf = new SparkConf()
       .set("spark.akka.frameSize", "128")
      .set("spark.kryoserializer.buffer.mb", "256")
       .set("spark.default.parallelism", "120")
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


    val queryString = "{\"filter\":{\"range\":{\"timestamp\":{\"from\":\"" + start_time + "\",\"to\":\"" + stop_time + "\"}}}}"



    val sourceMachineSpeed = "cdnlog.boss.speed.order/" + compute_minutes_es_type()
    val sourceMachineSpeedRDD = sc.esRDD(sourceMachineSpeed, queryString).map(_._2)

    val tupleLongReduce = (pair:(Long, Long, String, String), reduce: (Long, Long, String, String)) => {
      (pair._1 + reduce._1, pair._2 + reduce._2, pair._3, pair._4)
    }

    val bossSpeedCountTask = () => {
      val hourMachineSpeedCountTask = sourceMachineSpeedRDD.map(minutesResult => {
        val platformName = minutesResult.get("platform").get.toString()
        val platformId = minutesResult.get("popId").get.toString()
        val machineName = minutesResult.get("machineName").get.toString()
        val machineIsp = minutesResult.get("machineIsp").get.toString()
        val machinePrv = minutesResult.get("machinePrv").get.toString()
        val hitType = minutesResult.get("hitType").get.toString()
        val domain = minutesResult.get("domain").get.toString()
        val reqCount = minutesResult.get("reqCount").get.toString().toLong
        val speed = minutesResult.get("speed").get.toString().toLong

        val key = (machineName, domain, hitType, machineIsp, machinePrv)
        val value = (reqCount, speed, platformName, platformId)

        (key, value)
      }).reduceByKey(tupleLongReduce)
        .map(hourResult => {
        val storeResult = Map("platform" -> hourResult._2._3,
          "popId" -> hourResult._2._4,
          "machineName" -> hourResult._1._1,
          "machineIsp" -> hourResult._1._4,
          "machinePrv" -> hourResult._1._5,
          "hitType" -> hourResult._1._3,
          "domain" -> hourResult._1._2,
          "timestamp" -> getTimestamp().toString,
          "reqCount" -> hourResult._2._1,
          "speed" -> hourResult._2._2)

        storeResult
      }).saveToEs(computeEsIndexAndType("cdnlog.boss.speed.order."))

      hourMachineSpeedCountTask
    }


    sc.stop()
  }
}