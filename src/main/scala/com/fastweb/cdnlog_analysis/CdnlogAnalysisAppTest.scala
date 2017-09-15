package com.fastweb.cdnlog_analysis

import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import java.util.Calendar
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Partition
import java.util.ArrayList
import java.util.Date

object CdnlogAnalysisAppTest {
  case class Record(key: String)
  def main(args: Array[String]) {
    //val log = Logger.getRootLogger()
    if (args.length < 4) {
      System.err.println("Usage: CdnlogAnalysisApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    System.setProperty("java.io.tmpdir", "/hadoop/tmp")

    val format = new SimpleDateFormat("yyyyMMddHHmm")
    val Array(zkQuorum, group, topics, numThreads) = args

    println(zkQuorum, group, topics)

    val sparkConf = new SparkConf()
      .setAppName("CdnlogAnalysisAppTest")
      .set("spark.akka.frameSize", "1024")
      .set("spark.kryoserializer.buffer.mb", "512")
      // .set("spark.executor.memory", "32g")
      //    .set("spark.local.dir", "/data2/streaming/tmp/")
      //.set("spark.default.parallelism", "32")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "600")
      .set("spark.scheduler.mode", "FAIR")
      //.set("spark.storage.blockManagerSlaveTimeoutMs", "8000000")
      //.set("spark.storage.blockManagerHeartBeatMs", "8000000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.storage.memoryFraction", "0.9")
      // .set("spark.kryo.registrator", "cn.com.fastweb.cconap.log_result_registrator")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "10000")
      .set("spark.shuffle.manager", "SORT")
      // .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .set("spark.scheduler.allocation.file", "/data1/pool.xml")
      .set("spark.streaming.receiver.maxRate", "120000")

    val sc = new SparkContext(sparkConf)
    sc.setLocalProperty("spark.scheduler.pool", "cdnlog_pool_1")

    val ssc = new StreamingContext(sc, Seconds(60))

    val sqlContext = new SQLContext(sc)
    import sqlContext._
    //  ssc.checkpoint("hdfs:///checkpoint/")

    //    val kafkaRDD2 = topics.split(",").map ( x => {
    //        val topic = Map(x -> numThreads.toInt)
    //        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topic).map(_._2)
    //        
    //        lines
    //    })

    val kafka_config = Map(
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> "spark_streaming_cdnlog_3420001",
      "auto.commit.enable" -> "false",
      // "rebalance.backoff.ms" -> "8000",
      "auto.offset.reset" -> "largest")

    val topic_list = topics.split(",")
    val kafkaRDD = (0 to 11).map(i => {
      val topic = Map(topic_list(i / 2) -> 1)
      val kafka_stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafka_config, topic, StorageLevel.MEMORY_ONLY_SER).map(x => Record(x._2))
      kafka_stream
    })

    val lines = ssc.union(kafkaRDD) //.repartition(120) //.window(Seconds(5)).map(Record(_)).filter(_ != null)
    //lines.s
    def foreeachRdd = (rdd: RDD[Record], time: Time) => {
      var timestmp = format.format(new Date(time.milliseconds))
      try {
        //val tableName = "cdn_log_record_" + format.format(Calendar.getInstance.getTime)
        //sqlContext.createTable[Record](tableName, true)
        //rdd.insertInto("")
        rdd.saveAsParquetFile("/user/hive/warehouse/cdn_log_test/timestmp=" + timestmp)
      } catch {
        case e: Throwable =>
      }

      val hiveConf = new HiveConf()
      val hiveClient = new HiveMetaStoreClient(hiveConf)
      try {
        val newPartition = hiveClient.appendPartition("default", "cdn_log_test", "timestmp=" + timestmp)
      } catch {
        case e: Throwable =>
      } finally {
        hiveClient.close()
      }
    }
    lines.foreachRDD(foreeachRdd)

    ssc.start()
    ssc.awaitTermination()
  }
}