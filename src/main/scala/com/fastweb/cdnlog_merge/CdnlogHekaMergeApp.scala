package com.fastweb.cdnlog_merge

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Minutes
import kafka.serializer.StringDecoder
import java.util.ArrayList
import java.util.Date
import java.util.Locale
import org.apache.spark.streaming.Time

object CdnlogMergeApp {
  def main(args: Array[String]) {
    val oriTag = "ori\":"
    val endTag = "\",\""
    val hekaLog = """{"a":"113.121.193.14","RHeaderContentRange":"-","Fs":200,"pno":"1010","ru":"http://pcvideofast.titan.mgtv.com/mp4/2015/zongyi/kldby_12722/958426E4459F13895D63A379F05B346D_20150620_1_1_383.mp4/370000_380000_v01_mp4.ts?uuid=5b4cadbc55b84af19a1085e30d1f75a6&t=5739d7d1&pno=1010&sign=8a56f49c0922a6f4715bf73a29fb52fa&win=300&srgid=25005&urgid=1371&srgids=25005&nid=25005&payload=usertoken%3dhit%3d0&rdur=21600&arange=0&limitrate=0&fid=958426E4459F13895D63A379F05B346D&ver=0x03","@timestamp":"2016-05-16T06:30:22.000Z","cb":"565880","lt":1.463380222E18,"rm":"GET","HeaderXForwardedFor":"-","ori":"113.121.193.14 - - [16/May/2016:14:30:22 +0800] \"GET http://pcvideofast.titan.mgtv.com/mp4/2015/zongyi/kldby_12722/958426E4459F13895D63A379F05B346D_20150620_1_1_383.mp4/370000_380000_v01_mp4.ts?uuid=5b4cadbc55b84af19a1085e30d1f75a6&t=5739d7d1&pno=1010&sign=8a56f49c0922a6f4715bf73a29fb52fa&win=300&srgid=25005&urgid=1371&srgids=25005&nid=25005&payload=usertoken%3dhit%3d0&rdur=21600&arange=0&limitrate=0&fid=958426E4459F13895D63A379F05B346D&ver=0x03 HTTP/1.1\" 200 566262 565880 378.257 \"-\" \"-\" \"Yunfan Android 1.0.0.11\" \"-\" \"-\" 1010 \"FCACHE_HIT_DISK\" HNTV_FWCDN_TAG HAOMIAO 0.000 0.000 - - - - 0.000 378.162 378.162 -\n","st":"FCACHE_HIT_DISK","ra":"378.257","hostname":"ctl-zj-122-226-213-235.cdn.fastweb.com.cn","ua":"Yunfan Android 1.0.0.11","rv":"HTTP/1.1","cs":566262,"rf":"-","@version":"1","type":"hunantv_pno"}"""
    if (args.length != 5) {
      print("args list: zkQuorum, group, topics,timeoutString,duationString(in minutes)")
      sys.exit(0)
    }

    val Array(zkQuorum, group, topics, timeoutString, duationString) = args

    val logTimeout = timeoutString.toLong * 1000

    val duration = duationString.toLong;

    val sparkConf = new SparkConf().setAppName("CdnlogMergeApp")

    sparkConf.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .setAppName("CdnlogMergeApp")
      .set("spark.akka.frameSize", "1024")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", 60 * 60 * 24 + "")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.default.parallelism", "12")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.streaming.blockInterval", "60000")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.streaming.receiver.maxRate", "70000")
      .set("spark.ui.port", "4044")
      .set("spark.streaming.backpressure.enabled","true")
      .set("spark.streaming.stopGracefullyOnShutdown","true")

    val kafkaConfig = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "fetch.message.max.bytes" -> (1024 * 1024 * 10 * 3 + ""),
      "auto.commit.enable" -> "true",
      "auto.offset.reset" -> "largest",
      "zookeeper.sync.time.ms" -> "2000")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(duration * 60))

    val topicList = topics.split(",")
    var topicSize = topicList.size * 2
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 2) -> 1)
      val kafka_stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaConfig, topic, StorageLevel.DISK_ONLY).map(x => {
          try {
            val value = x._2
            val oriStartIndex = value.indexOf(oriTag)
            var oriEndIndex = value.indexOf(endTag, oriStartIndex)
            value.substring(oriStartIndex + oriTag.length() + 1, oriEndIndex)
          } catch {
            case e: Throwable => e.printStackTrace(); null
          }
        }).filter { x => x != null }
      kafka_stream
    })

    val logs = ssc.union(kafkaRDD)

    val format = new SimpleDateFormat("yyyyMMddHHmm")

    //   val timeFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

    def foreeachRdd = (rdd: RDD[String], time: Time) => {
      var timestmp = format.format(new Date(time.milliseconds - duration * 60 * 1000))
      var year = timestmp.substring(0, 4)
      var month = timestmp.substring(4, 6)
      var day = timestmp.substring(6, 8)
      var hour = timestmp.substring(8, 10)
      var minute = timestmp.substring(10, 12)
      try {
        rdd.saveAsTextFile("/user/cdnlog_heka/" + year + "/" + month + "/" + day + "/" + hour + "/" + minute)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
    logs.foreachRDD(foreeachRdd)
    ssc.start()
    ssc.awaitTermination()
  }
}