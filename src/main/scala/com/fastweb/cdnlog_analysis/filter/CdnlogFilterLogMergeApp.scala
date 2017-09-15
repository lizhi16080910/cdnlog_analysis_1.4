package com.fastweb.cdnlog_analysis.filter

import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random}

import kafka.message.MessageAndMetadata
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.filter.FilterKafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}


object CdnlogFilterLogMergeApp {
    def main(args: Array[String]) {
        if (args.length != 4) {
            println("usage:receiverMaxRate zkQuorum, group, topics")
            sys.exit(0)
        }
        val Array(receiverMaxRate, zkQuorum, group, topics) = args

        val sparkConf = new SparkConf().setAppName("CdnlogFilterLogMergeApp")

        sparkConf.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.streaming.unpersist", "true")
            .set("spark.storage.memoryFraction", "0.45")
            .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
            .set("spark.shuffle.manager", "SORT")
            .set("spark.streaming.receiver.maxRate", receiverMaxRate)
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
        var timeb = 0l;

        /**
          *  按域名过滤的方法
          */
        val filter = (msg:MessageAndMetadata[String,String]) => {
            val domains = Domains.apply()
            val line = msg.message()
            val domainStartIndex = line.indexOf("://") + 3
            if (domainStartIndex < 0) {
                return false
            }
            val domainEndIndex = line.indexOf("/", domainStartIndex)
            if (domainEndIndex < 0) {
                return false
            }
            val domain = line.substring(domainStartIndex, domainEndIndex)
            if (domains.contains(domain)) {
                return true
            }
            false
        }

        val topicList = topics.split(",")
        var topicSize = topicList.size * 2
        val kafkaRDD = (0 to topicSize - 1).map(i => {
            val topic = Map(topicList(i / 2) -> 1)
            val kafka_stream = FilterKafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
                ssc, kafkaConfig, topic, StorageLevel.DISK_ONLY,classOf[DomainFilter]).map(_._2)
                      .map(r => LogTrim(r)).filter(line => !line.equals(""))
            kafka_stream
        })

        val logs = ssc.union(kafkaRDD)

        /**
          *
          * @return 写数据到hdfs的方法
          */
        def foreeachRdd = (rdd: RDD[String], time: Time) => {
            val format = new SimpleDateFormat("yyyy/MM/dd/HH/mm")
            var timestmp = format.format(new Date(time.milliseconds))
            try {
                rdd.saveAsTextFile("/user/cdnlog_filter/" + timestmp)
            } catch {
                case e: Throwable => e.printStackTrace()
            }
        }

        /**
          * 写多玩的数据到kafka,cdnlog_duowan
          */
        logs.foreachRDD(rdd => {
            rdd.foreachPartition(lines => {
                val random = new Random();
                val props = new Properties()
                props.put("metadata.broker.list", "192.168.100.3:9092,192.168.100.4:9092,192.168.100.5:9092,192.168.100.6:9092")
                props.put("serializer.class", "kafka.serializer.StringEncoder")
                props.put("key.serializer.class", "kafka.serializer.StringEncoder")
                props.put("request.required.acks", "-1");
                props.put("compression.codec", "snappy")
                props.put("producer.type", "async")
                props.put("topic.metadata.refresh.interval.ms", "30000")
                props.put("batch.num.messages", "1000")
                props.put("retry.backoff.ms", "8000")

                val config = new ProducerConfig(props)
                val producer = new Producer[String, String](config)

                try {
                    lines.foreach(line => {
                        val kmessage = new KeyedMessage[String, String]("cdnlog_duowan", String.valueOf(random.nextInt()), line)
                        producer.send(kmessage)
                    })
                } catch {
                    case t: Throwable => t.printStackTrace()
                } finally {
                    producer.close()
                }
            })
        })

        /**
          * 写多玩的数据到hdfs
          */
        logs.foreachRDD(foreeachRdd)
        // 添加监控
        ssc.addStreamingListener(new JobListener("/data2/log_filter_merge/count_records"))
        ssc.start()
        sys.addShutdownHook({
            println("shut down hook stop streaming context")
            ssc.stop(true, true)
            println("success stop streaming context ")
        })
        ssc.awaitTermination()
    }



}