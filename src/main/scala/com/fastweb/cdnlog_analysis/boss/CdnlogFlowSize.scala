package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat
import java.util.Date


import com.fastweb.cdnlog_analysis.{Channel, IPArea, LogInfo}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sparkRDDFunctions

/**
 * cdnlog prelong 回源分析。
 *
 *
 *
 */

object CdnlogFlowSize {

  def main(args: Array[String]) {

    val Array(zkQuorum, group, topics,esNode) = args

    val conf = new SparkConf()
      .setAppName("CdnlogFlowSize")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "256")
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
        .filter { x => x != null}
      kafka_stream
    })

    val lines = ssc.union(kafkaRDD) //.repartition(partition_num.toInt)
    val formate = new SimpleDateFormat("yyyyMMddHH")
    val windowLine = lines.window(Minutes(1), Minutes(1)).persist()

    val currentTimestamp = System.currentTimeMillis() / 1000
    val sourceWindowLine = windowLine.filter(x => !LogInfo.isTimeout(x, currentTimestamp))

    val  transformHitType =(hitType:Int) => {
      /*just count back to parent, back source and hit three hit type*/
      if (hitType.equals(3)){
           1
       } else {
         hitType
      }
    }

    sourceWindowLine.map(x => {
      val minutesTimestamp = LogInfo.getTimestampByMinutesTimestamp(x.timestamp)
      val key  = (x.domain, x.isp, x.prv, x.hitType, x.userid, minutesTimestamp)
      val value = (x.es, x.flowSize, x.reqNum, x.zeroSpeedReqNum)
      (key, value)
    }).reduceByKey((x1: (Double, Long, Long, Long), x2: (Double, Long, Long, Long)) => {
      (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4)
    }).foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        val area = IPArea.update().getPrvAndCountry(x._1._3)
        Map("domain" -> x._1._1,
          "isp" -> x._1._2,
          "city" -> x._1._3,
          "prv" -> area._1,
          "country" -> area._2,
          "hit_type" -> transformHitType(x._1._4),
          "userid" -> x._1._5,
          "timestamp" -> x._1._6,
          "speed_size" -> LogInfo.getSpeed(x._2._1,x._2._2).toInt,
          "es" -> x._2._1,
          "flow_size" -> x._2._2,
          "req_count" -> x._2._3,
          "zeroSpeedReqNum" -> x._2._4)
      })
      //result.saveToEs("cdnlog.flow.size.count.five." + indexPart.substring(0,8)+"/"+indexPart)
      result.saveToEs("cdnlog.flow.size.count/"+indexPart)
    })
/*
     windowLine.map(x => ((x.domain, x.timestamp, x.ua, x.userid), (x.flowSize, x.reqNum)))
      .reduceByKey((x1: (Long, Long), x2: (Long, Long)) => (x1._1 + x2._1, x1._2 + x2._2))
      .foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        Map("domain" -> x._1._1,
          "timestamp" -> x._1._2,
          "ua" -> x._1._3,
          "userid" -> x._1._4,
          "flow_size" -> x._2._1,
          "req_count" -> x._2._2)
      })
      result.saveToEs("cdnlog.ua/" + indexPart)
    })*/


    //超时处理

    val timeoutSourceWindowLine = windowLine.filter(x => LogInfo.isTimeout(x, currentTimestamp))

    timeoutSourceWindowLine.map(x => {
      val minitesTimestamp = LogInfo.getHourTimestampBySecondTimestamp(x.timestamp)
      val key = (x.domain, x.isp, x.prv,  x.hitType, x.userid, minitesTimestamp)
      val value = (x.es, x.flowSize, x.reqNum, x.zeroSpeedReqNum)
      (key, value)
    }).reduceByKey((x1: (Double, Long, Long, Long), x2: (Double, Long, Long, Long)) =>
      (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4))
      .foreachRDD((rdd, time) => {
      val result = rdd.map(x => {
        val area = IPArea.update().getPrvAndCountry(x._1._3)
        Map("domain" -> x._1._1,
          "isp" -> x._1._2,
          "city" -> x._1._3,
          "prv" -> area._1,
          "country" -> area._2,
          "hit_type" -> transformHitType(x._1._4),
          "userid" -> x._1._5,
          "speed_size" -> LogInfo.getSpeed(x._2._1,x._2._2).toInt,
          "es" -> x._2._1,
          "flow_size" -> x._2._2,
          "req_count" -> x._2._3,
          "zeroSpeedReqNum" -> x._2._4,
          "timestamp" -> x._1._6)
      })
      result.saveToEs("cdnlog.flow.size.count.hour/timeout")
    })

    //流量统计，五分钟超时处理
    timeoutSourceWindowLine.map(x =>
      ((x.domain, x.isp, x.prv, LogInfo.getFiveTimestampBySecondTimestamp(x.timestamp.toLong), x.hitType, x.userid),
        (x.es, x.flowSize, x.reqNum, x.zeroSpeedReqNum)))
      .reduceByKey((x1: (Double, Long, Long, Long), x2: (Double, Long, Long, Long)) => (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4))
      .foreachRDD((rdd, time) => {
      val result = rdd.map(x => {
        val area = IPArea.update().getPrvAndCountry(x._1._3)
        Map("domain" -> x._1._1,
          "isp" -> x._1._2,
          "city" -> x._1._3,
          "prv" -> area._1,
          "country" -> area._2,
          "timestamp" -> x._1._4,
          "hit_type" -> transformHitType(x._1._5),
          "userid" -> x._1._6,
          "speed_size" -> LogInfo.getSpeed(x._2._1,x._2._2).toInt,
          "es" -> x._2._1,
          "flow_size" -> x._2._2,
          "req_count" -> x._2._3,
          "zeroSpeedReqNum" -> x._2._4)
      })
      result.saveToEs("cdnlog.flow.size.count.five/timeout")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}