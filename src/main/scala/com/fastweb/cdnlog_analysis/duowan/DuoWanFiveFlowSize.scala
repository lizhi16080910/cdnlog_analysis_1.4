package com.fastweb.cdnlog_analysis.duowan

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.filter.FilterKafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sparkRDDFunctions

/**
 * Created by liuming on 2017/3/2.
  * 多玩5分钟带宽
 */

object DuoWanFiveFlowSize {

  def main(args: Array[String]) {

    val Array(zkQuorum, group, topics, esNode, maxRate) = args
    val conf = new SparkConf()
      .setAppName("DuoWanFiveFlowSize")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer", "64")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "30000")
      .set("es.index.auto.create", "true")
      .set("spark.default.parallelism", "24")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.receiver.maxRate", maxRate)
      .set("es.nodes", esNode)

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Minutes(5))

    val kafka_config = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group)

    val topicList = topics.split(",")
    var topicSize = topicList.size * 12
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 12) -> 1)
      val kafka_stream = FilterKafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
        .map(x => DuowanLog(x._2))
        .filter { x => x != null }
      kafka_stream
    })

    val lines = ssc.union(kafkaRDD)
    val windowLine = lines.window(Minutes(5), Minutes(5))

    val formate = new SimpleDateFormat("yyyyMMdd")

    windowLine.map(x => {
      val key = (x.domain, x.timestamp,x.https_flag)
      val value = (x.flow_size, 1L)
      (key, value)
    }).reduceByKey((x1: (Long, Long), x2: (Long, Long)) => {
      (x1._1 + x2._1, x1._2 + x2._2)
    }).foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        Map("domain" -> x._1._1,
          "timestamp" -> x._1._2,
          "https_flash" -> x._1._3,
          "flow_size" -> x._2._1,
          "req_count" -> x._2._2,
          "userid" -> "1159")
      })
      result.saveToEs("cdnlog.duowan.flowsize" + "/" + indexPart)
      result.saveToEs("cdnlog.domain.flow.size.count" + "/" + "duowan_" + indexPart)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
