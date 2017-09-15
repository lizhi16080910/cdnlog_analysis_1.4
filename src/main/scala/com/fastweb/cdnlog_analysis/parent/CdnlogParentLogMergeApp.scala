package com.fastweb.cdnlog_analysis.parent

import java.text.SimpleDateFormat
import java.util.Date

import com.fastweb.cdnlog_analysis.filter.{DomainFilter, JobListener, LogTrim}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.filter.FilterKafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

object CdnlogParentLogMergeApp {
    def main(args: Array[String]) {
        if (args.length != 3) {
            println("usage:zkQuorum, group, topics")
            sys.exit(0)
        }
        val Array(zkQuorum, group, topics) = args

        val sparkConf = new SparkConf().setAppName("CdnlogDuowanParentLogMergeApp")

        sparkConf.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.streaming.unpersist", "true")
            .set("spark.storage.memoryFraction", "0.45")
            .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
            .set("spark.shuffle.manager", "SORT")
            .set("spark.streaming.receiver.maxRate", "30000")
            .set("spark.streaming.unpersist", "true")
            .set("spark.cleaner.ttl", 60 * 60 * 24 + "")
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.default.parallelism", "72")
            .set("spark.rdd.compress", "true")
            .set("spark.shuffle.compress", "true")
            .set("spark.streaming.blockInterval", "30000")
            .set("spark.shuffle.manager", "SORT")
            .set("spark.ui.port", "4046")

        val kafkaConfig = Map(
            "zookeeper.connect" -> zkQuorum,
            "group.id" -> group,
            "fetch.message.max.bytes" -> (1024 * 1024 * 10 * 3 + ""),
            "auto.commit.enable" -> "true",
            "auto.offset.reset" -> "largest",
            "zookeeper.sync.time.ms" -> "8000")

        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(1 * 60))

        val topicList = topics.split(",")
        var topicSize = topicList.size * 2
        val kafkaRDD = (0 to topicSize - 1).map(i => {
            val topic = Map(topicList(i / 2) -> 1)
            val kafka_stream = FilterKafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
                ssc, kafkaConfig, topic, StorageLevel.DISK_ONLY, classOf[DomainFilter]).map(_._2)
                .map(r => LogTrim(r)).filter(line => !line.equals(""))
            kafka_stream
        })
        val logs = ssc.union(kafkaRDD)

        def foreeachRdd = (rdd: RDD[String], time: Time) => {
            val format = new SimpleDateFormat("yyyy/MM/dd/HH/mm")
            var timestmp = format.format(new Date(time.milliseconds))
            try {
                rdd.saveAsTextFile("/user/cdnlog_parent/" + timestmp)
            } catch {
                case e: Throwable => e.printStackTrace()
            }
        }
        logs.foreachRDD(foreeachRdd)
        ssc.addStreamingListener(new JobListener("/data2/cdnlog_analysis/parent_log_merge/count_records"))
        ssc.start()
        sys.addShutdownHook({
            println("shut down hook stop streaming context")
            ssc.stop(true, true)
            println("success stop streaming context ")
        })
        ssc.awaitTermination()
    }
}