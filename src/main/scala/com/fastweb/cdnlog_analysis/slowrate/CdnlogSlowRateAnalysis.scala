package com.fastweb.cdnlog_analysis.slowrate

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.filter.slowrate.SlowRateFilterKafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sparkRDDFunctions

/**
  * cdnlog prelong 回源分析。
  *
  *
  *
  */
object CdnlogSlowRateAnalysis {

    def main(args: Array[String]) {

        val Array(zkQuorum, group, topics, esNode) = args

        val conf = new SparkConf()
            .setAppName("CdnlogSlowRateAnalysis")
            .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.akka.frameSize", "256")
            .set("spark.kryoserializer.buffer.mb", "512")
            .set("spark.streaming.unpersist", "true")
            .set("spark.cleaner.ttl", "36000")
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.rdd.compress", "true")
            .set("spark.shuffle.compress", "true")
            .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
            .set("spark.streaming.blockInterval", "60000")
            .set("spark.shuffle.manager", "SORT")
            .set("es.index.auto.create", "true")
            .set("spark.default.parallelism", "12")
            .set("spark.streaming.receiver.maxRate", "80000")
            .set("es.nodes", esNode)

        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(60 * 5))

        val kafka_config = Map(
            "zookeeper.connect" -> zkQuorum,
            "group.id" -> group,
            "auto.commit.enable" -> "false",
            "auto.offset.reset" -> "largest"
        )

        val topicList = topics.split(",")
        var topicSize = topicList.size * 12
        val kafkaRDD = (0 to topicSize - 1).map(i => {
            val topic = Map(topicList(i / 12) -> 1)
            val kafka_stream = SlowRateFilterKafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
                ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
            kafka_stream
        })

        val line = ssc.union(kafkaRDD)
        val lines = line.map(line => SlowRateLogInfo(line._2.trim))
            .map(x => {
                x match {
                    case Left(msg) => msg
                    case Right(result) => null
                }
            }).filter(x => x != null)
        //val windowLine = lines.window(Minutes(5), Minutes(5)).persist()
        val format = new SimpleDateFormat("yyyyMMdd")

        lines.map(x => {
            val key = (x.domain, x.host, x.isp, x.province, x.time, x.ifSlow)
            val value = 1l
            (key, value)
        }).reduceByKey(_ + _).foreachRDD((rdd, time) => {
            val indexPart = format.format(new Date(time.milliseconds))
            val result = rdd.map(x => {
                Map("domain" -> x._1._1,
                    "host" -> x._1._2,
                    "isp" -> x._1._3,
                    "province" -> x._1._4,
                    "time" -> x._1._5,
                    "ifSlow" -> x._1._6,
                    "count" -> x._2
                )
            })
            result.saveToEs("cdnlog.rate.slow.percent" + "/" + indexPart)
            //rdd.saveAsTextFile("/user/lifq/slow_rate/" + indexPart)
        })
        ssc.start()
        ssc.awaitTermination()
    }
}