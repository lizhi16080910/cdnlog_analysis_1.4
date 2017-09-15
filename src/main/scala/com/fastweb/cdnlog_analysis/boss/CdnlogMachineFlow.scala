package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat
import java.util.Date

import com.fastweb.cdnlog_analysis.{BossLogInfo, IPArea, Channel, LogInfo}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sparkRDDFunctions

/**
 * Created by zery on 15-8-20.
 */

object CdnlogMachineFlow {


  def main(args: Array[String]) {
    val Array(zkQuorum, group, topics,esNode) = args

    val conf = new SparkConf()
      .setAppName("CdnlogPrelogMachineFlow")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "15000")
      .set("spark.shuffle.manager", "SORT")
      .set("es.index.auto.create", "true")
      .set("spark.default.parallelism", "60")
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
        .filter { x => x != null && IPArea.update().hasCityCode(x.prv)}
      kafka_stream
    })

    val lines = ssc.union(kafkaRDD) //.repartition(partition_num.toInt)

    val hoursTime = new SimpleDateFormat("yyyyMMddHH")
    val currentTimestamp = System.currentTimeMillis() / 1000
    val windowLine = lines.window(Minutes(1), Minutes(1)).persist()

    val sourceDstream = windowLine.filter(x => !LogInfo.isTimeout(x, currentTimestamp))
    val timeoutSourceDstream = windowLine.filter(rdd => LogInfo.isTimeout(rdd, (System.currentTimeMillis() / 1000)))

    val ispPrvflowReduce = (pair: (Long, String, Int, Int, Int), reduce: (Long, String, Int, Int, Int)) => {
      (pair._1 + reduce._1, pair._2, pair._3, pair._4, pair._5)
    }

    val computeTotalFlow = (sourceDstream: DStream[BossLogInfo]) => {
      val result = sourceDstream.map(source => {
        val platformHost = source.machineId
        val platformName = source.platform
        val platformID = source.popID
        val platformIsp = source.machineIsp
        val platformPrv = source.machinePrv
        val minutesTimestamp = LogInfo.getTimestampByMinutesTimestamp(source.timestamp)
        val flowSize = source.flowSize
        val domain = source.domain
        val requestIsp = source.isp
        val requestPrv = source.prv
        val platformIspCode = source.machineIspCode
        val platformPrvCode = source.machinePrvCode

        val key = (platformHost, platformIsp, platformPrv, domain, requestIsp, requestPrv, minutesTimestamp)
        val value = (flowSize, platformName, platformID, platformIspCode, platformPrvCode)

        (key, value)
      }).reduceByKey(ispPrvflowReduce)
        .map(countResult => {
        val platHost = countResult._1._1
        val platIsp = countResult._1._2
        val platPrv = countResult._1._3
        val domain = countResult._1._4
        val requestIsp = countResult._1._5
        val requestPrv = countResult._1._6
        val timestamp = countResult._1._7

        val flowSize = countResult._2._1
        val platName = countResult._2._2
        val platId = countResult._2._3
        val platIspCode = countResult._2._4
        val platPrvCode = countResult._2._5

        val requestArea = IPArea.update().getPrvAndCountry(requestPrv)
        val provincePrvCode = requestArea._1

        val totalFlowResult = Map("platform" -> platName,
        "popId" -> platId.toString(),
        "machineName" -> platHost,
        "domain" -> domain,
        "machineIsp" -> platIsp,
        "machineIspCode" -> platIspCode.toString(),
        "machinePrv" -> platPrv,
        "machinePrvCode" -> platPrvCode.toString(),
        "isp" -> requestIsp,
        "prv" -> provincePrvCode,
        "timestamp" -> timestamp.toString(),
        "flowSize" -> flowSize)

        totalFlowResult
      })

      result
    }

    val flowReduce = (pair: (Long, Long, String, Int), reduce: (Long, Long, String, Int)) => {
      (pair._1 + reduce._1, pair._2 + reduce._2, pair._3, pair._4)
    }

    val computeFlowReqnumByHitype = (sourceDstream : DStream[BossLogInfo]) => {
      val result = sourceDstream.map(source => {
        val platformHost = source.machineId
        val platformName = source.platform
        val platformID = source.popID
        val platformIsp = source.machineIsp
        val platformPrv = source.machinePrv
        val minutesTimestamp = LogInfo.getTimestampByMinutesTimestamp(source.timestamp)
        val reqNum = source.reqNum
        val flowSize = source.flowSize
        val hitType = source.hitType
        val domain = source.domain

        val key = (platformHost, platformIsp, platformPrv, domain, hitType, minutesTimestamp)
        val value = (reqNum, flowSize, platformName, platformID)

        (key, value)
      }).reduceByKey(flowReduce)
        .map(result => {
        val platHost = result._1._1
        val platIsp = result._1._2
        val platPrv = result._1._3
        val domain = result._1._4
        val hitType = result._1._5
        val timestamp = result._1._6

        val reqNum = result._2._1
        val flowSize = result._2._2
        val platName = result._2._3
        val platID = result._2._4


        val flowCountResult = Map("platform" -> platName,
          "popId" -> platID.toString(),
          "machineName" -> platHost,
          "machineIsp" -> platIsp,
          "machinePrv" -> platPrv,
          "hitType" -> hitType,
          "reqCount" -> reqNum,
          "flowSize" -> flowSize,
          "domain" -> domain,
          "timestamp" -> timestamp.toString())

        flowCountResult
      })

      result
    }

    val es_type = () => {
      hoursTime.format(new Date(System.currentTimeMillis()))
    }

    val machineFlowReqnumCountTask = computeFlowReqnumByHitype(sourceDstream)
      .foreachRDD(_.saveToEs("cdnlog.boss.hit.count/" + es_type()))



    val machineTotalFlowCountTask = computeTotalFlow(sourceDstream)
      .foreachRDD(_.saveToEs("cdnlog.boss.flowsize/" + es_type()))



    ssc.start()
    ssc.awaitTermination()
  }
}
