package com.fastweb.cdnlog_analysis


import java.sql.{DriverManager, Statement, Connection}
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Date

import com.fastweb.cdnlog_analysis.isms.IsmsFilter
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Minutes
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.filter.FilterKafkaUtils


object HekaCdnlogAnalysis_c06i06 extends Logging {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Usage: HekaCdnlogAnalysis_c06i06 <zkQuorum> <group> <topics> <maxRate> <uiPort>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, maxRate, uiPort) = args
    println(zkQuorum, group, topics, maxRate)

    val sparkConf = new SparkConf()
      .setAppName("HekaCdnlogAnalysis_c06i06")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.storage.memoryFraction", "0.45")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.ui.port", uiPort)
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "10000")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.streaming.receiver.maxRate", maxRate)
      .set("spark.default.parallelism", "72")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Minutes(2))
    val sqlContext = new SQLContext(sc)
    import sqlContext._

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
      val kafka_stream = FilterKafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
      kafka_stream
    })

    val logInfos = ssc.union(kafkaRDD).map(x => { LogInfo(x._2.trim(), Channel.update()) }).filter { x => x != null }

    val format = new SimpleDateFormat("yyyyMMddHHmm")
    def foreeachRdd = (rdd: RDD[LogInfo], time: Time) => {
      
      var timestmp = format.format(new Date(time.milliseconds))
      var month = timestmp.substring(0, 6)
      var day = timestmp.substring(6, 8)
      var hour = timestmp.substring(8, 10)
      var minute = timestmp.substring(10, 12) + "0"
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

    def ismsForeeachRdd = (rdd: RDD[LogInfo], time: Time) => {
      val table = "cdnlog_isms_monitor_filter2"
      var timestmp = format.format(new Date(time.milliseconds))
      var month = timestmp.substring(0, 6)
      var day = timestmp.substring(6, 8)
      var hour = timestmp.substring(8, 10)
      var minute = timestmp.substring(10, 12) + "0"
      try {
        rdd.saveAsParquetFile(s"/user/hive/warehouse/${table}/month_=" + month + "/day_=" + day + "/hour_=" + hour +
            "/minute_=" + minute)
      } catch {
        case e: Throwable => this.logError(s"save parquet failed:/user/hive/warehouse/${table}/month_=${month}/day_=${day}/hour_=${hour}/minute_=${minute}", e)
      }
//      val ip = "30"
//      var conn: Connection = null
//      var statement: Statement = null
//      try {
//        Class.forName("org.apache.hive.jdbc.HiveDriver");
//        conn = DriverManager.getConnection("jdbc:hive2://192.168.100." + ip + ":21050/default;auth=noSasl")
//        statement = conn.createStatement()
//        var rs = statement.execute(s"alter table ${table} add partition(month_='" + month + "', day_='" + day + "', " +
//            "hour_='" + hour + "', minute_='" + minute + "')")
//      } catch {
//        case t: Throwable => t.printStackTrace()
//      } finally {
//        if (statement != null) {
//          statement.close()
//        }
//        if (conn != null) {
//          conn.close()
//        }
//      }

      val hiveConf = new HiveConf()
      val hiveClient = new HiveMetaStoreClient(hiveConf)
      val partitionValues = new ArrayList[String]()
      partitionValues.add(month)
      partitionValues.add(day)
      partitionValues.add(hour)
      partitionValues.add(minute)
      try {
        val newPartition = hiveClient.appendPartition("default", table, partitionValues)
      } catch {
        case e: Throwable =>
      } finally {
        hiveClient.close()
      }
    }
    logInfos.window(Minutes(2), Minutes(2)).filter(logInfo => IsmsFilter.filterDomain(logInfo.domain)).foreachRDD(ismsForeeachRdd)

    ssc.start()
    ssc.awaitTermination()

    /*
    sys.addShutdownHook({
      this.logInfo("shut down hook stop streaming context")
      ssc.stop(true, true)
      this.logInfo("success stop streaming context ")
    })
    */
  }
}