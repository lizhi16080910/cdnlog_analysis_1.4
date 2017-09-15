package com.fastweb.cdnlog_analysis.sql

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
import com.fastweb.cdnlog_analysis.LogInfo
import com.fastweb.cdnlog_analysis.Channel
import org.elasticsearch.spark._
object CdnlogStreamSqlApp {
  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println("Usage: CdnlogSqlApp <zkQuorum> <group> <topics> <esNode> <esIndex> <sqlStr>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, esNode, esIndex, sqlStr) = args
    println(zkQuorum, group, topics, esNode, esIndex, sqlStr)

    val sparkConf = new SparkConf()
      .setAppName("CdnlogStreamSqlApp" + ":\t" + esIndex + ":\t" + sqlStr)
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.storage.memoryFraction", "0.45")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "20000")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.streaming.receiver.maxRate", "7000")
      .set("spark.default.parallelism", "74")
      .set("es.index.auto.create", "true")
      .set("es.nodes", esNode)

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(300))
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val kafka_config = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.commit.enable" -> "true",
      "fetch.message.max.bytes" -> ("" + 1024 * 1024 * 10 * 2 * 15),
      "auto.offset.reset" -> "largest")

    val topicList = topics.split(",")
    var topicSize = topicList.size * 2
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 2) -> 1)

      val kafka_stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.DISK_ONLY)
      kafka_stream
    })

    def foreeachRdd = (rdd: RDD[LogInfo], time: Time) => {
      val formate = new SimpleDateFormat("yyyyMMddHH")
      var indexPart = formate.format(new Date(time.milliseconds))
      rdd.registerTempTable("loginfo")
      sql(sqlStr).toJSON.saveJsonToEs(esIndex + "/" + indexPart)
    }

    val logInfos = ssc.union(kafkaRDD).map(x => { LogInfo(x._2, Channel.update()) }).filter { x => x != null }.window(Seconds(300), Seconds(300))

    logInfos.foreachRDD(foreeachRdd)
    ssc.start()
    ssc.awaitTermination()
  }
}