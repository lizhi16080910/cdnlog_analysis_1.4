package com.fastweb.cdnlog_analysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.spark.sparkRDDFunctions
import kafka.serializer.StringDecoder
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.filter.DirCsKafkaUtils
object CdnlogDirCsApp extends Logging {
  def main(args: Array[String]) {
    
    if (args.length != 5) {
      System.err.println("Usage: CdnlogDirCsApp <zkQuorum> <group> <topics> <esNode> <maxRate>")
      System.exit(1)
    }
    
    val Array(zkQuorum, group, topics, esNode, maxRate) = args
    System.setProperty("java.io.tmpdir", "/hadoop/temp")

    val sparkConf = new SparkConf().setAppName("CdnlogDirCsApp")

    sparkConf.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "256")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", 24 * 60 * 60 + "")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.default.parallelism", "36")
      .set("spark.storage.memoryFraction", "0.45")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "20000")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.streaming.receiver.maxRate", maxRate)
      .set("spark.ui.port", "4046")
      .set("es.index.auto.create", "true")
      .set("es.nodes", esNode)
      .set("es.resource", "dir_cs/domain")

    val kafkaConfig = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "fetch.message.max.bytes" -> ("" + 1024 * 1024 * 2 * 15),
      "auto.commit.enable" -> "true",
      "auto.offset.reset" -> "largest",
      "rebalance.backoff.ms" -> "10000",
      "rebalance.max.retries" -> "10",
      "zookeeper.connection.timeout.ms" -> "1000000",
      "zookeeper.session.timeout.ms" -> "20000",
      "zookeeper.sync.time.ms" -> "10000")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))

    val topicList = topics.split(",")
    var topicSize = topicList.size * 4
    val kafkaRDD = (0 to topicSize - 1).map(i => {
      val topic = Map(topicList(i / 4) -> 1)
      val kafka_stream = DirCsKafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConfig, topic, StorageLevel.DISK_ONLY)
      kafka_stream
    })

    val dirCs = ssc.union(kafkaRDD).map(x => { var dircs = DirCs(x._2, Channel.update()); dircs }).filter(x => x != null)

    def foreachFunc = (rdd: RDD[((String, String, String, String), Long)], time: Time) => {
      val formate = new SimpleDateFormat("yyyyMMddHH")
      var indexPart = formate.format(new Date(time.milliseconds))
      rdd.map(x => Map("domain" -> x._1._1, "dir1" -> x._1._2, "dir2" -> x._1._3, "timestamp" -> DirCs.timestamp2Date(x._1._4), "cs" -> x._2)).saveToEs("dir_cs/" + indexPart)
    }

    var dirCsWindow = dirCs.window(Minutes(1), Minutes(1))

    var dir = dirCsWindow.filter(x => x.pno.equals("")).map(x => ((x.domain, x.dir1, x.dir2, x.timestmp), x.cs)).reduceByKey((x1: Long, x2: Long) => x1 + x2).foreachRDD(foreachFunc)

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