package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat
import java.util.Date

import com.fastweb.cdnlog_analysis.{Channel, LogInfo}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sparkRDDFunctions
/**
 *  5分钟状态码统计
 *
 *
 *
 */

object CdnlogFiveMinuteStatusCount {

  // If you do not see this printed, that means the StreamingContext has been loaded
  // from the new checkpoint
  println("Creating new context")
  def createContext(zkQuorum:String,group:String,topics:String,esNode:String,checkpointDirectory:String):StreamingContext={
    val conf = new SparkConf()
      .setAppName("CdnlogFiveMinuteStatusCount")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "6000")
      .set("spark.shuffle.manager", "SORT")
      .set("es.index.auto.create", "true")
      .set("spark.default.parallelism", "120")
      .set("spark.streaming.receiver.maxRate", "80000")
      .set("es.nodes", esNode)
    //.set("spark.streaming.receiver.writeAheadLog.enable","true")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(300))
    ssc.checkpoint(checkpointDirectory)

    val kafka_config = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group
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
      val key  = (x.domain, x.userid,x.statusCode,minutesTimestamp)
      val value = 1
      (key, value)
    }).reduceByKey((x1, x2 ) => {
      (x1 + x2)
    }).foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        Map("domain" -> x._1._1,
          "userid" -> x._1._2,
          "status_code" -> x._1._3,
          "req_count" -> x._2,
          "timestamp" -> x._1._4)
      })
      result.saveToEs("cdnlog.domain.status.code.count."  + indexPart.substring(0,8)+"/"+indexPart)
    })
    ssc
  }

  def main(args: Array[String]) {

    val Array(zkQuorum, group, topics,esNode,checkpointDirectory) = args
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(zkQuorum, group, topics,esNode,checkpointDirectory)
      })

    ssc.start()
    ssc.awaitTermination()
  }
}