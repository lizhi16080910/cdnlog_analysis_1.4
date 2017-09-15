package com.fastweb.cdnlog_analysis


import java.text.SimpleDateFormat
import java.util
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}



object CdnlogJsonAnalysisApp {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: CdnlogJsonAnalysisApp <zkQuorum> <group> <topics>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics) = args
    println(zkQuorum, group, topics)

    val sparkConf = new SparkConf()
      .setAppName("jsonLogAnalysisApp")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "512")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "600")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.streaming.receiver.maxRate","70000")
      .set("spark.default.parallelism","72")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val kafka_config = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group)

    val topicList = topics.split(",")
    var topicSize = topicList.size * 12
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 12) -> 1)

      val kafka_stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
      kafka_stream
    })
    val logInfos = ssc.union(kafkaRDD).map(x => { JsonLogInfo(x._2, Channel.update()) }).filter { x => x != null }.repartition(12)

    val format = new SimpleDateFormat("yyyyMMddHHmm")

    def foreeachRdd = (rdd: RDD[JsonLogInfo], time: Time) => {
      var timestmp = format.format(new Date(time.milliseconds))
      var month = timestmp.substring(0, 6)
      var day = timestmp.substring(6, 8)
      var hour = timestmp.substring(8, 10)

      var minute = timestmp.substring(10, 12)

      try {
         rdd.saveAsParquetFile("/user/hive/warehouse/cdnlog_json/month_=" + month + "/day_=" + day + "/hour_=" + hour + "/minute_=" + minute)
        //rdd.saveAsTextFile("user/hive/warehouse/cdnlog_json/month_=" + month + "/day_=" + day + "/hour_=" + hour + "/minute_=" + minute)
      } catch {
        case e: Throwable =>
      }


      val hiveConf = new HiveConf()
      val hiveClient = new HiveMetaStoreClient(hiveConf)
      val partitionValues = new util.ArrayList[String]()
      partitionValues.add(month)
      partitionValues.add(day)
      partitionValues.add(hour)
      partitionValues.add(minute)
      try {
        val newPartition = hiveClient.appendPartition("default", "cdnlog_json", partitionValues)
      } catch {
        case e: Throwable =>
      } finally {
        hiveClient.close()
      }
    }
    logInfos.foreachRDD(foreeachRdd)
    ssc.start()
    ssc.awaitTermination()
  }
}