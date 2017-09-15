package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat

import com.fastweb.cdnlog_analysis.{Channel, LogInfo}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.elasticsearch.spark.sparkRDDFunctions
import java.util.Date

/**
 *  5分钟流量统计
 *
 *
 *
 */

object CdnlogPrelogSourceAnalysis {

  def main(args: Array[String]) {

    val Array(zkQuorum, group, topics,esNode) = args

    val conf = new SparkConf()
      .setAppName("TempFlowSizeApp")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.mb", "64")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.scheduler.mode", "FAIR")
      //.set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      //.set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "1000")
      .set("spark.shuffle.manager", "SORT")
      .set("es.index.auto.create", "true")
      .set("spark.default.parallelism", "120")
      .set("spark.streaming.receiver.maxRate", "80000")
      .set("es.nodes", esNode)

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(300))

    val kafka_config = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group ,//"auto.commit.enable" -> "false",
     "fetch.message.max.bytes"->"1000000"
      //"auto.offset.reset" -> "largest"
   // "socket.timeout.ms" -> "60000"
      )


    val topicList = topics.split(",")
    var topicSize = topicList.size * 2
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 2 ) -> 1)
      val kafka_stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
        .map(x => LogInfo(x._2, Channel.update(),"csCount"))
        .filter { x => x != null}
      kafka_stream
    })


    val lines = ssc.union(kafkaRDD) //.repartition(partition_num.toInt)

    val formate = new SimpleDateFormat("yyyyMMddHH")


    val windowLine = lines.window(Minutes(5), Minutes(5))


    windowLine.map(x => {
      val minutesTimestamp = LogInfo.getFiveTimestampBySecondTimestamp(x.timestmp)
      val key  = (x.domain, x.userid,x.isp,x.prv,x.area_pid,x.area_cid,x.statusCode, minutesTimestamp,x.platformName,x.popID)
      val value = ( x.cs, 1)
      (key, value)
    }).reduceByKey((x1, x2 ) => {
      (x1._1 + x2._1, x1._2 + x2._2)
    }).foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        Map("domain" -> x._1._1,
          "userid" -> x._1._2,
          "isp" -> x._1._3,
          "prv" -> x._1._4,
          "area_pid" -> x._1._5,
        "area_cid"->x._1._6,
          "status" ->  x._1._7,
          "timestamp" -> x._1._8,
        "platformName" -> x._1._9,
        "popID"-> x._1._10,
          "flow_size" -> x._2._1,
          "req_count" -> x._2._2)
      })
      result.saveToEs("temp.cdnlog.flow.size.count.five." + indexPart.substring(0,8)+"/"+indexPart)
    })


    /*windowLine.map(x => {
      val minutesTimestamp = LogInfo.getFiveTimestampBySecondTimestamp(LogInfo.timeToLong(x.timestmp).toString)
      val key  = (x.domain,   x.userid, minutesTimestamp)
      val value = ( x.cs, 1)
      (key, value)
    }).reduceByKey((x1, x2 ) => {
      (x1._1 + x2._1, x1._2 + x2._2)
    }).foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {

        Map("domain" -> x._1._1,
          "userid" -> x._1._2,
          "timestamp" -> (x._1._3.toLong),
          "flow_size" -> x._2._1,
          "req_count" -> x._2._2)
      })
      result.saveToEs("temp.cdnlog.flow.size.count/" + indexPart)
    })*/

/*
  按天划分索引
    val currentTimestamp = System.currentTimeMillis() / 1000

    val sourceDstream = windowLine.filter(x => !LogInfo.isTimeout(x, currentTimestamp))
    val timeoutSourceDstream = windowLine.filter(rdd => LogInfo.isTimeout(rdd, (System.currentTimeMillis() / 1000)))




    sourceDstream.map(x => {
      val minutesTimestamp = LogInfo.getFiveTimestampBySecondTimestamp(LogInfo.timeToLong(x.timestmp).toString)
      val key  = (x.domain, x.userid,x.isp,x.prv,x.status, minutesTimestamp)
      val value = ( x.cs, 1)
      (key, value)
    }).reduceByKey((x1, x2 ) => {
      (x1._1 + x2._1, x1._2 + x2._2)
    }).foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {

        Map("domain" -> x._1._1,
          "userid" -> x._1._2,
          "isp" -> x._1._3,
          "prv" -> x._1._4,
          "status" ->  x._1._5,
          "timestamp" -> (x._1._6.toLong),
          "flow_size" -> x._2._1,
          "req_count" -> x._2._2)
      })
      result.saveToEs("temp.cdnlog.flow.size.count.five." + indexPart.substring(0,8)+"/"+ indexPart)
    })

    timeoutSourceDstream.map(x => {
      val minutesTimestamp = LogInfo.getFiveTimestampBySecondTimestamp(LogInfo.timeToLong(x.timestmp).toString)
      val key  = (x.domain, x.userid,x.isp,x.prv,x.status, minutesTimestamp)
      val value = ( x.cs, 1)
      (key, value)
    }).reduceByKey((x1, x2 ) => {
      (x1._1 + x2._1, x1._2 + x2._2)
    }).foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {

        Map("domain" -> x._1._1,
          "userid" -> x._1._2,
          "isp" -> x._1._3,
          "prv" -> x._1._4,
          "status" ->  x._1._5,
          "timestamp" -> (x._1._6.toLong),
          "flow_size" -> x._2._1,
          "req_count" -> x._2._2)
      })
      result.saveToEs("temp.cdnlog.flow.size.count.five/"+ indexPart)
    })
*/


    ssc.start()
    ssc.awaitTermination()
  }
}