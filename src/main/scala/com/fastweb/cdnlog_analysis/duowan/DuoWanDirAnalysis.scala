package com.fastweb.cdnlog_analysis.duowan

import java.text.SimpleDateFormat
import java.util.Date
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.filter.FilterKafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.elasticsearch.spark.sparkRDDFunctions
/**
 * Created by liuming on 2017/3/2.
  * 多玩目录分析
 */


object DuoWanDirAnalysis {

  def main(args: Array[String]) {

    val Array(zkQuorum, group, topics,esNode,maxRate) = args
    val conf = new SparkConf()
      .setAppName("DuoWanDirAnalysis")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer", "64")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.scheduler.mode","FAIR")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "30000")
      .set("es.index.auto.create", "true")
      .set("spark.default.parallelism", "24")
      .set("spark.streaming.backpressure.enabled","true")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
      .set("spark.streaming.receiver.maxRate", maxRate)
      .set("es.nodes", esNode)

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Minutes(5))

    val kafka_config = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group
    )

    val topicList = topics.split(",")
    val topicSize = topicList.size * 10
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 10 ) -> 1)
      val kafka_stream = FilterKafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
        .map(x => DuowanLog(x._2))
        .filter { x => x != null}
      kafka_stream
    })

    val lines = ssc.union(kafkaRDD)
    val windowLine = lines.window(Minutes(5), Minutes(5))

    val format = new SimpleDateFormat("yyyyMMdd")

    windowLine.map(x => {
      val key  = (x.domain,x.dir, x.timestamp,x.https_flag)
      val value = ( x.flow_size,1L)
      (key, value)
    }).reduceByKey((x1: (Long, Long), x2: (Long, Long)) => {
      (x1._1 + x2._1, x1._2 + x2._2)
    }).foreachRDD((rdd, time) => {
      val indexPart = format.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        Map("domain" -> x._1._1,
          "dir" -> x._1._2,
          "timestamp" -> x._1._3,
          "https_flag" -> x._1._4,
          "flow_size" -> x._2._1,
          "req_count" -> x._2._2)
      })
      result.saveToEs("cdnlog.duowan.dir.flowsize"  + "/"+indexPart)
    })

    ssc.start()
    ssc.awaitTermination()

    }
}
