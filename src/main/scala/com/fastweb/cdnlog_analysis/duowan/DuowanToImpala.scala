package com.fastweb.cdnlog_analysis.duowan

import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Date
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import com.fastweb.cdnlog_analysis.Channel
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.filter.FilterKafkaUtils

/**
  * Created by liuming on 2017/3/2.
  * 多玩入impala
  */

object DuowanToImpala extends Logging {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: DuowanToImpala <zkQuorum> <group> <topics> <maxRate>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, maxRate) = args
    println(zkQuorum, group, topics, maxRate)

    val sparkConf = new SparkConf()
      .setAppName("DuowanToImpala")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.storage.memoryFraction", "0.45")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "60000")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.streaming.receiver.maxRate", maxRate)
      .set("spark.default.parallelism", "144")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "hdfs://nameservice1/user/spark/applicationHistory/")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Minutes(5))
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
    val topicSize = topicList.size * 10
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 10) -> 1)

      val kafka_stream = FilterKafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
      kafka_stream
    })

    val logInfos = ssc.union(kafkaRDD).map(x => {
      DuowanLogInfo(x._2.trim(), Channel.update())
    })
//    logInfos.persist(StorageLevel.DISK_ONLY)
    val normalDstream = logInfos.map(x => x match {
      case Left(msg)     => msg
      case Right(result) => null
    }).filter(x => x != null)

    val errorDstream = logInfos.map(x => x match {
      case Left(msg)     => null
      case Right(result) => result
    }).filter(x => x != null).repartition(1)

    val format = new SimpleDateFormat("yyyyMMddHHmm")
    def normaleachRdd = (rdd: RDD[DuowanLogInfo], time: Time) => {

      val timestmp = format.format(new Date(time.milliseconds - 300000))
      val month = timestmp.substring(0, 6)
      val day = timestmp.substring(6, 8)
      val hour = timestmp.substring(8, 10)
      val minute =timestmp.substring(10, 12)
      try {
        rdd.saveAsParquetFile("/user/hive/warehouse/cdnlog_duowan/month_=" + month + "/day_=" + day + "/hour_=" + hour + "/minute_=" + minute)
      } catch {
        case e: Throwable => this.logError(s"save parquet failed:/user/hive/warehouse/cdnlog_duowan/month_=${month}/day_=${day}/hour_=${hour}/minute_=${minute}", e)
      }
      val hiveConf = new HiveConf()
      val hiveClient = new HiveMetaStoreClient(hiveConf)
      val partitionValues = new ArrayList[String]()
      partitionValues.add(month)
      partitionValues.add(day)
      partitionValues.add(hour)
      partitionValues.add(minute)
      try {
        val newPartition = hiveClient.appendPartition("default", "cdnlog_duowan", partitionValues)
      } catch {
        case e: Throwable =>
      } finally {
        hiveClient.close()
      }
    }
        def erroreachRdd = (rdd: RDD[String], time: Time) => {

      val timestmp = format.format(new Date(time.milliseconds-300000))
      val month = timestmp.substring(0, 6)
      val day = timestmp.substring(6, 8)
      val hour = timestmp.substring(8, 10)
      val minute = timestmp.substring(10, 12)
      try {
        rdd.saveAsTextFile("/user/cdnlog_duowan/error/month_=" + month + "/day_=" + day + "/hour_=" + hour + "/minute_=" + minute)
      } catch {
        case e: Throwable => this.logError(s"save hdfs failed:/user/cdnlog_duowan/error/month_=${month}/day_=${day}/hour_=${hour}/minute_=${minute}", e)
      }
      
    }
//        normalDstream.foreachRDD(rdd => println(rdd.count))
//        errorDstream.foreachRDD(rdd => rdd.foreach (println))
    normalDstream.window(Minutes(5), Minutes(5)).foreachRDD(normaleachRdd)
    errorDstream.window(Minutes(5), Minutes(5)).foreachRDD(erroreachRdd)
    ssc.start()
    sys.addShutdownHook({
      this.logInfo("shut down hook stop streaming context")
      ssc.stop(true, true)
      this.logInfo("success stop streaming context ")
    })
    ssc.awaitTermination()
  }
}
