/**
 * Created by zery on 15-8-17.
 */
package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat
import java.util.Date

import com.fastweb.cdnlog_analysis.{BossLogInfo, Channel, LogInfo}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sparkRDDFunctions

object CdnlogMachineCompute {

  def main(args: Array[String]) {
    val Array(zkQuorum, group, topics,esNode) = args

    val conf = new SparkConf()
      .setAppName("CdnlogMachineCompute")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "128")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "500")
      .set("spark.shuffle.manager", "SORT")
      .set("es.index.auto.create", "true")
      .set("spark.default.parallelism", "120")
      .set("spark.streaming.receiver.maxRate", "80000")
      .set("es.nodes", esNode)

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(60))

    val kafka_config = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group //"auto.commit.enable" -> "false",
      //"auto.offset.reset" -> "largest"
    )

    val topicList = topics.split(",")
    var topicSize = topicList.size * 2
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 2 ) -> 1)
      val kafka_stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
        .map(x => LogInfo(x._2, Channel.update(), platformInfo.update()))
        .filter { x => x != null /* && IPArea.update().hasCityCode(x.prv)*/ && x.speedSize != 0L }
      kafka_stream
    })

    val lines = ssc.union(kafkaRDD) //.repartition(partition_num.toInt)

    val hoursTime = new SimpleDateFormat("yyyyMMddHH")
    val currentTimestamp = System.currentTimeMillis() / 1000
    val windowLine = lines.window(Minutes(1), Minutes(1))//.persist()

    val sourceDstream = windowLine.filter(x => !LogInfo.isTimeout(x, currentTimestamp))
    val timeoutSourceDstream = windowLine.filter(rdd => LogInfo.isTimeout(rdd, (System.currentTimeMillis() / 1000)))

    /*
    val uaReduce = (pair: (Long, String, Int), reduce: (Long, String, Int)) => {
      (pair._1 + reduce._1, pair._2, pair._3)
    }


    val statusCodeCount = (sourceDstream : DStream[BossLogInfo]) => {
      val result = sourceDstream.map(source => {
        val platformHost = source.machineId
        val platformName = source.platform
        val platformID = source.popID
        val platformIsp = source.machineIsp
        val platformPrv = source.machinePrv
        val reqNum = source.reqNum
        val minutesTimestamp = LogInfo.getTimestampByMinutesTimestamp(source.timestamp)
        val statusCode = source.statusCode

        val key = (platformHost, statusCode, platformIsp, platformPrv, minutesTimestamp)
        val value = (reqNum, platformName, platformID)
        (key, value)
      }).reduceByKey(uaReduce)
        .map(result => {
        val platformHost = result._1._1
        val statusCode = result._1._2
        val platformIsp = result._1._3
        val platformPrv = result._1._4
        val timestamp = result._1._5
        val countResult = result._2._1
        val platformName = result._2._2
        val platformID = result._2._3

        val esDataStruct = Map("platform" -> platformName,
          "popId" -> platformID.toString(),
          "machineName" -> platformHost,
          "machineIsp" -> platformIsp,
          "machinePrv" -> platformPrv,
          "statusCode" -> statusCode,
          "reqCount" -> countResult,
          "timestamp" -> timestamp.toString())

        esDataStruct
      })

      result
    }
    */

    val speedReduce = (pair: (Long, Double, String, Int), reduce: (Long, Double, String, Int)) => {
      (pair._1 + reduce._1, pair._2 + reduce._2, pair._3, pair._4)
    }

    val speedCount = (sourceDstream : DStream[BossLogInfo]) => {
      val result = sourceDstream.map(source => {
        val platformHost = source.machineId
        val platformName = source.platform
        val platformID = source.popID
        val platformIsp = source.machineIsp
        val platformPrv = source.machinePrv
        val minutesTimestamp = LogInfo.getTimestampByMinutesTimestamp(source.timestamp)
        val speed = source.speedSize
        val hitType = source.hitType
        val domain = source.domain
        val reqNum = source.reqNum //- source.zeroSpeedReqNum

        val key = (platformHost, domain, hitType, platformIsp, platformPrv, minutesTimestamp)
        val value = (reqNum, speed, platformName, platformID)

        (key, value)
      }) //.filter(_._2._1 != 0L)
        .reduceByKey(speedReduce)
        .map(result => {
        val platHost = result._1._1
        val domain = result._1._2
        val hitType = result._1._3
        val platIsp = result._1._4
        val platPrv = result._1._5
        val timestamp = result._1._6
        val reqNum = result._2._1
        val speed = result._2._2
        val platName = result._2._3
        val platID = result._2._4

        val esSpeedResult = Map("platform" -> platName,
          "popId" -> platID.toString(),
          "machineName" -> platHost,
          "machineIsp" -> platIsp,
          "machinePrv" -> platPrv,
          "hitType" -> hitType,
          "domain" -> domain,
          "timestamp" -> timestamp.toString(),
          "reqCount" -> reqNum,
          "speed" -> speed)

        esSpeedResult
      })

      result
    }

    val es_type = () => {
      hoursTime.format(new Date(System.currentTimeMillis()))
    }
/*
    val machineStatusCodeCountTask = statusCodeCount(sourceDstream)
      .foreachRDD(_.saveToEs("cdnlog.boss.status.code/" + es_type()))*/

    val machineSpeedCountTask = speedCount(sourceDstream)
      .foreachRDD(_.saveToEs("cdnlog.boss.speed.order/" + es_type()))


    val storeTimeoutMachineSpeedCountData = speedCount(timeoutSourceDstream)
      .map(result => {
      val timestamp = result.get("timestamp").get.toString().toLong
      val hourTimestamp = (timestamp / 3600 * 3600).toString()
      val updateResult = result.updated("timestamp", hourTimestamp)
      updateResult
    }).foreachRDD(_.saveToEs("cdnlog.boss.speed.order.hour/timeout"))

   /* val storeTimeoutMachineStatusCodeData = statusCodeCount(timeoutSourceDstream)
      .map(result => {
      val timestamp = result.get("timestamp").get.toString().toLong
      val hourTimestamp = (timestamp / 3600 * 3600).toString()
      val updateResult = result.updated("timestamp", hourTimestamp)
      updateResult
    }).foreachRDD(_.saveToEs("cdnlog.boss.status.code.hour/timeout"))*/

    ssc.start()
    ssc.awaitTermination()
  }


}
