package com.fastweb.cdnlog_analysis

import java.io.File
import java.io.FileReader
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Date
import java.util.Properties
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.reflect.runtime.universe
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import java.io.FileInputStream
import org.apache.spark.streaming.Minutes
import org.apache.spark.Logging
object CdnlogAnalysisApp extends Logging {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: CdnlogAnalysisApp <zkQuorum> <group> <topics> < >")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, maxRate) = args
    println(zkQuorum, group, topics, maxRate)

    val sparkConf = new SparkConf()
      .setAppName("CdnlogAnalysisApp")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.storage.memoryFraction", "0.45")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "10000")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.streaming.receiver.maxRate", maxRate)
      .set("spark.default.parallelism", "144")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Minutes(2))
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    //ssc.checkpoint("/checkpoint/")

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
      val topic = Map(topicList(i / 12) -> 12)

      val kafka_stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
      kafka_stream
    })

    val logInfos = ssc.union(kafkaRDD).map(x => { LogInfo(x._2.trim(), Channel.update()) }).filter { x => x != null }

    val format = new SimpleDateFormat("yyyyMMddHHmm")
    def foreeachRdd = (rdd: RDD[LogInfo], time: Time) => {
      
      var timestmp = format.format(new Date(time.milliseconds))
      var month = timestmp.substring(0, 6)
      var day = timestmp.substring(6, 8)
      var hour = timestmp.substring(8, 10)
      var minute = timestmp.substring(10, 12)
      try {
        rdd.saveAsParquetFile("/user/hive/warehouse/cdnlog/month_=" + month + "/day_=" + day + "/hour_=" + hour + "/minute_=" + minute)
      } catch {
        case e: Throwable => this.logError(s"save parquet failed:/user/hive/warehouse/cdnlog/month_=${month}/day_=${day}/hour_=${hour}/minute_=${minute}", e)
      }
      val hiveConf = new HiveConf()
      val hiveClient = new HiveMetaStoreClient(hiveConf)
      val partitionValues = new ArrayList[String]()
      partitionValues.add(month)
      partitionValues.add(day)
      partitionValues.add(hour)
      partitionValues.add(minute)
      try {
        val newPartition = hiveClient.appendPartition("default", "cdnlog", partitionValues)
      } catch {
        case e: Throwable =>
      } finally {
        hiveClient.close()
      }
    }
    logInfos.window(Minutes(2), Minutes(2)).foreachRDD(foreeachRdd)
    ssc.start()
    sys.addShutdownHook({
      this.logInfo("shut down hook stop streaming context")
      ssc.stop(true, true)
      this.logInfo("success stop streaming context ")
    })
    ssc.awaitTermination()
  }
}