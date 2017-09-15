package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat
import java.util.Date

import com.fastweb.cdnlog_analysis.{IPArea, LogInfo}
import com.fastweb.cdnlog_analysis.{Channel}
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

object CdnlogStatusCodeSpeed {

  def main(args: Array[String]) {
    val Array(zkQuorum, group, topics,esNode) = args

    val conf = new SparkConf()
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
    val sourceMachineDomainIspPrvFlowCount = windowLine

    /**
     * 处理状态码
     */
    sourceMachineDomainIspPrvFlowCount.map(x => {
      val key = (x.domain, x.isp, x.prv, x.timestamp, x.statusCode, x.userid)
      val value = (x.flowSize, x.reqNum)
      (key, value)
    }).reduceByKey((x1: (Long, Long), x2: (Long, Long)) => (x1._1 + x2._1, x1._2 + x2._2))
      .foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.
        map(x => {
        val area = IPArea.update().getPrvAndCountry(x._1._3)
        Map("domain" -> x._1._1,
          "isp" -> x._1._2,
          "city" -> x._1._3,
          "prv" -> area._1,
          "country" -> area._2,
          "timestamp" -> x._1._4,
          "status_code" -> x._1._5,
          "userid" -> x._1._6,
          "flow_size" -> x._2._1,
          "req_count" -> x._2._2)
      })
      result.saveToEs("cdnlog.status.code/" + indexPart)
    })

    /*
    val tuple12_reduce = (reduce: (Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long),
                          pair: (Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long)) => {
      (reduce._1 + pair._1, reduce._2 + pair._2, reduce._3 + pair._3, reduce._4 + pair._4, reduce._5 + pair._5,
        reduce._6 + pair._6, reduce._7 + pair._7, reduce._8 + pair._8, reduce._9 + pair._9, reduce._10 + pair._10,
        reduce._11 + pair._11, reduce._12 + pair._12)
    }

    /**
     * 处理速度区间
     */
    sourceMachineDomainIspPrvFlowCount.map { x =>
      val speeds = LogInfo.getSpeedInterval(x)
      ((x.domain,LogInfo.getFiveTimestampBySecondTimestamp(x.timestamp), x.userid),
        (x.flowSize, x.reqNum, speeds(0), speeds(1), speeds(2), speeds(3), speeds(4),
          speeds(5), speeds(6), speeds(7), speeds(8), speeds(9)))
    }.reduceByKey(tuple12_reduce)
      .foreachRDD { (rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        val area = IPArea.update().getPrvAndCountry(x._1._3)
        Map("domain" -> x._1._1,
          "timestamp" -> x._1._2,
          "userid" -> x._1._3,
          "flow_size" -> x._2._1,
          "req_count" -> x._2._2,
          "speed0" ->x._2._3,
          "speed1" ->x._2._4,
          "speed2" ->x._2._5,
          "speed3" ->x._2._6,
          "speed4" ->x._2._7,
          "speed5" ->x._2._8,
          "speed6" ->x._2._9,
          "speed7" ->x._2._10,
          "speed8" ->x._2._11,
          "speed9" ->x._2._12)
      })
      result.saveToEs("cdnlog.speed/" + indexPart)
    }
    }*/






    ssc.start()
    ssc.awaitTermination()
  }
}