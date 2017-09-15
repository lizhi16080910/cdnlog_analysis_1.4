package com.fastweb.cdnlog_analysis.boss

import java.text.SimpleDateFormat
import java.util.Date
import com.fastweb.cdnlog_analysis.{ Channel, LogInfo }
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.filter.FilterKafkaUtils
import org.apache.spark.streaming.{ Minutes, Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.elasticsearch.spark.sparkRDDFunctions

/**
 *  5分钟流量统计
 */
object HekaCdnlogFiveMinuteFlowSize_c01i05 {

  def main(args: Array[String]) {

    if (args.length != 6) {
      System.err.println("Usage: HekaCdnlogFiveMinuteFlowSize_c01i05 <zkQuorum> <group> <topics> <esNode> <maxRate> <uiPort>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, esNode, maxRate, uiPort) = args

    val conf = new SparkConf()
      .setAppName("HekaCdnlogFiveMinuteFlowSize_c01i05")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "3600")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "20000")
      .set("spark.shuffle.manager", "SORT")
      .set("es.index.auto.create", "true")
      .set("spark.ui.port", uiPort)
      .set("spark.default.parallelism", "60")
      .set("spark.streaming.receiver.maxRate", maxRate)
      .set("es.nodes", esNode)

    val sc = new SparkContext(conf)
    val formate = new SimpleDateFormat("yyyyMMdd")
    val ssc = new StreamingContext(sc, Seconds(300))

    val kafka_config = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.commit.enable" -> "true",
      "fetch.message.max.bytes" -> ("" + 1024 * 1024 * 10 * 3),
      "auto.offset.reset" -> "largest",
      "rebalance.backoff.ms" -> "10000",
      "rebalance.max.retries" -> "10",
      "zookeeper.connection.timeout.ms" -> "1000000",
      "zookeeper.session.timeout.ms" -> "20000",
      "zookeeper.sync.time.ms" -> "10000")

    val topicList = topics.split(",")
    var topicSize = topicList.size * 12
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 12) -> 1)
      val kafka_stream = FilterKafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafka_config, topic, StorageLevel.DISK_ONLY).map(x => LogInfo(x._2.trim(), Channel.update(), "csCount")).filter { x => x != null }
      kafka_stream
    })

    val lines = ssc.union(kafkaRDD)

    val windowLine = lines.window(Minutes(5), Minutes(5))

    windowLine.map(x => {
      val minutesTimestamp = LogInfo.getFiveTimestampBySecondTimestamp(x.timestmp)
      val key = (x.domain, x.userid, minutesTimestamp)
      val value = x.cs
      (key, (value, 1))
    }).reduceByKey((x1, x2) => {
      (x1._1 + x2._1, x1._2 + x2._2)
    }).foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        Map("domain" -> x._1._1,
          "userid" -> x._1._2,
          "timestamp" -> (x._1._3), //五分钟时间戳
          "flow_size" -> x._2._1, //流量
          "req_count" -> x._2._2) //请求数
      })
      result.saveToEs("cdnlog.domain.flow.size.count" + "/" + indexPart)
    })

    /*   
    windowLine.filter { x => 
      FilterUserUtils.update().checkUserID(x.userid)
    }.map(x => {
      val minutesTimestamp = LogInfo.getFiveTimestampBySecondTimestamp(x.timestmp)
      val key = (x.domain, x.userid, x.statusCode, minutesTimestamp)
      val value = 1
      (key, (x.cs, value))
    }).reduceByKey((x1, x2) => {
      (x1._1 + x2._1, x1._2 + x2._2)
    }).foreachRDD((rdd, time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        Map("domain" -> x._1._1,
          "userid" -> x._1._2,
          "status_code" -> x._1._3, //状态码
          "timestamp" -> x._1._4,   //五分钟时间戳
          "flow_size" -> x._2._1,   //流量
          "req_count" -> x._2._2)   //请求数
      })
      result.saveToEs("cdnlog.domain.status.code.count" + "/" + indexPart)
    })
*/

/*    windowLine.filter { x => (x.cs >= 3 * 1024 * 1024) && x.node != 0 && x.es > 0.0 && x.es < 20000000.0 }.map(x => {
      val minutesTimestamp = LogInfo.getFiveTimestampBySecondTimestamp(x.timestmp)
      val key = (x.isp, x.prv, x.node)
      (key, (x.cs, x.es, 1))
    }).reduceByKey((x1, x2) => {
      (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3)
    }).foreachRDD((rdd, time) => {
      
      val fivetime = time.milliseconds / 1000 / 300 * 300 + 300
      
      val indexPart = formate.format(new Date(time.milliseconds))
      val result = rdd.map(x => {
        Map("isp" -> x._1._1,
          "prv" -> x._1._2,
          "node" -> x._1._3,
          "timestamp" -> fivetime,
          "cs" -> x._2._1,
          "es" -> x._2._2,
          "count" -> x._2._3)
      })
      result.saveToEs("cdnlog.isp.prv.node.speed" + "/" + indexPart)
    })*/

    
/*    windowLine.filter { x => (x.cs >= 3 * 1024 * 1024) && x.node != 0 && x.es > 0.0 && x.es < 20000000.0 }.map(x => {
      val key = (x.isp, x.prv, x.node)
      (key, x.cs / x.es)
    }).groupByKey().foreachRDD((rdd, time) => {
      
      val fivetime = time.milliseconds / 1000 / 300 * 300 + 300
      
    	val indexPart = formate.format(new Date(time.milliseconds))
      rdd.map(key_value => {
        val key = key_value._1
        val value = key_value._2.toList.sortWith(_>_)
        val size = value.size
        val middle = (0 + size) / 2
        (key, value(middle))
      }).map(x => {
        Map("isp" -> x._1._1, "prv" -> x._1._2, "node" -> x._1._3, "timestamp" -> fivetime, "speed" -> x._2)
      }).saveToEs("cdnlog.isp.prv.node.median.speed" + "/" + indexPart)
    })
*/    
    
    
        /*中位数*/
    val middleSpeed = windowLine.filter { x => (x.cs >= 3 * 1024 * 1024) && x.node != 0 && x.es > 0.0 && x.es < 20000000.0 }.map(x => {
      val key = (x.isp, x.prv, x.node)
      (key, x.cs / x.es)
    }).groupByKey().map(key_value => {
      val key = key_value._1
      val value = key_value._2.toList.sortWith(_ > _)
      val size = value.size
      val middle = (0 + size) / 2
      (key, value(middle))
    })

    /*平均下载速度*/
    val avgSpeed = windowLine.filter { x => (x.cs >= 3 * 1024 * 1024) && x.node != 0 && x.es > 0.0 && x.es < 20000000.0 }.map(x => {
      val key = (x.isp, x.prv, x.node)
      (key, (x.cs, x.es, 1))
    }).reduceByKey((x1, x2) => {
      (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3)
    })

    /*两Dstream Join*/
    avgSpeed.join(middleSpeed).foreachRDD((rdd, time) => {
      val fivetime = time.milliseconds / 1000 / 300 * 300 + 300
      val indexPart = formate.format(new Date(time.milliseconds))
      rdd.map(x => {
        Map("isp" -> x._1._1,
          "prv" -> x._1._2,
          "node" -> x._1._3,
          "timestamp" -> fivetime,
          "cs" -> x._2._1._1,
          "es" -> x._2._1._2,
          "count" -> x._2._1._3,
          "speed" -> x._2._2)
      }).saveToEs("cdnlog.isp.prv.node.join.speed" + "/" + indexPart)
    })
    
    ssc.start()
    ssc.awaitTermination()
  }

}