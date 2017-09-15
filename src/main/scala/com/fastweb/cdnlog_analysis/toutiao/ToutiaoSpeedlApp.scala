package com.fastweb.cdnlog_analysis.toutiao

import java.text.SimpleDateFormat
import java.util.Date

import com.fastweb.cdnlog_analysis.{IPArea, Channel, LogInfo}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.elasticsearch.spark.sparkRDDFunctions

object ToutiaoSpeedlApp {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Usage: CdnlogToutiaoSpeedApp  <zkQuorum>  <group>  <topics> <esNode> <domian>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics,esNode,domain) = args

    val sparkConf = new SparkConf()
      .setAppName("CdnlogToutiaoSpeedApp")
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
      .set("spark.streaming.receiver.maxRate", "80000")
      .set("es.nodes", esNode)
      .set("spark.default.parallelism", "60")


    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))

    val ispList = LogInfo.getResource("isp")
    //2015 1222 1734 60


    val kafka_config = Map(
      "zookeeper.connect" ->  zkQuorum,
      "group.id" -> group)

    val topicList = topics.split(",")
    var topicSize = topicList.size * 2
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 2 ) -> 1)
      val kafka_stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
        .filter { x => x._2.contains(domain)}.map{x => LogInfo(x._2, Channel.update())}.filter { x => x != null }
      kafka_stream
    })

    def speedTransform = (sourceDstream: DStream[LogInfo]) => {
      val result = sourceDstream.map(source =>{
        val hostname = source.machine
        val domain = source.domain
        //val prv = source.prv
        val isp = source.isp
        val ip = source.ip

        val city = String.valueOf(LogInfo.search(LogInfo.prvList, ip))
        val prv = IPArea.update().getPrvAndCountry(city)._1

        val userid = source.userid
        val timestamp = source.timestmp
        val cs = source.cs
        val es = source.es
        var ratio = "1"
        if(cs !=0 && cs/1024/es*1000 < 80){
          ratio = "0"
        }
        val serverip= LogInfo.ipToLong(source.Fa)
        val serverisp = LogInfo.search(ispList, ip).toString

        val key=(hostname,domain,prv,isp,ip,userid,timestamp,ratio,serverip,serverisp)
        val value=(1)

        (key,value)

      }).reduceByKey(_ + _).map(result =>{
        val hostname = result._1._1
        val domain = result._1._2
        val prv = result._1._3
        val isp = result._1._4
        val count = result._2
        val ratio = result._1._8
        val timestamp = LogInfo.timeToLong(result._1._7) / 60 * 60
        val fivetime = String.valueOf(timestamp/300 * 300 + 300)
        val ip = result._1._5
        val userid = result._1._6
        val serverip = result._1._9
        val serverisp = result._1._10

        val esSpeedResult = Map("hostname" -> hostname,
                                "domain" -> domain,
                                "prv" -> prv,
                                "isp" -> isp,
                                "count" -> count,
                                "ratio"->ratio,
                                "timestamp" -> timestamp.toString,
                                "fivetime" -> fivetime,
                                "ip"->ip,
                                "userid"->userid,
                                "serverip"->serverip,
                                "serverisp"->serverisp)
        esSpeedResult
      })
      result
    }





    val hoursTime = new SimpleDateFormat("yyyyMMdd")
    val es_type = () => {
      hoursTime.format(new Date(System.currentTimeMillis()))
    }

    val lines = ssc.union(kafkaRDD)
    val windowLine = lines.window(Minutes(1), Minutes(1))

    val Task = speedTransform(windowLine).
      foreachRDD(_.saveToEs("kscloud_speed/"+es_type() ))


    ssc.start()
    ssc.awaitTermination()
  }
}