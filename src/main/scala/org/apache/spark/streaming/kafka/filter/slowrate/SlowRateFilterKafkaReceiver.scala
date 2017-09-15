package org.apache.spark.streaming.kafka.filter.slowrate

import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.filter.{Filter, ThreadUtils}
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.Map
import scala.reflect._

/**
 * Created by lfq on 2016/7/25.
 */

class SlowRateFilterKafkaReceiver[K: ClassTag, V: ClassTag, U <: Decoder[_]: ClassTag, T <: Decoder[_]: ClassTag](
  kafkaParams: Map[String, String],
  topics: Map[String, Int],
  storageLevel: StorageLevel,
  filterClass: Class[_ <: Filter]) extends Receiver[(K, V)](storageLevel) with Logging {

  // Connection to Kafka
  var consumerConnector: ConsumerConnector = null

  def onStop() {
    if (consumerConnector != null) {
      consumerConnector.shutdown()
      consumerConnector = null
    }
  }

  def onStart() {

    logInfo("Starting Kafka Consumer Stream with group: " + kafkaParams("group.id"))

    // Kafka connection properties
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))

    val zkConnect = kafkaParams("zookeeper.connect")
    // Create the connection to the cluster
    logInfo("Connecting to Zookeeper: " + zkConnect)
    val consumerConfig = new ConsumerConfig(props)
    consumerConnector = Consumer.create(consumerConfig)
    logInfo("Connected to " + zkConnect)

    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[K]]
    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[V]]

    // Create threads for each topic/message Stream we are listening
    val topicMessageStreams = consumerConnector.createMessageStreams(topics, keyDecoder, valueDecoder)

    val executorPool = ThreadUtils.newDaemonFixedThreadPool(topics.values.sum, "KafkaMessageHandler")
    try {
      // Start the messages handler for each partition
      topicMessageStreams.values.foreach { streams =>
        streams.foreach { stream => executorPool.submit(new MessageHandler(stream, filterClass)) }
      }
    } finally {
      executorPool.shutdown() // Just causes threads to terminate after work is done
    }
  }

  // Handles Kafka messages
  private class MessageHandler(stream: KafkaStream[K, V], filterClass: Class[_ <: Filter])
    extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      try {
        val streamIterator = stream.iterator()
        while (streamIterator.hasNext()) {
          val msgAndMetadata = streamIterator.next()
          val message = msgAndMetadata.message()
          if(SlowRateFilterKafkaReceiver.filter(message.asInstanceOf[String])){
             store((msgAndMetadata.key, message))
          }
          //store((msgAndMetadata.key, msgAndMetadata.message()))
        }
      } catch {
        case e: Throwable => logError("Error handling message; exiting", e)
      }
    }
  }
}

object SlowRateFilterKafkaReceiver{

  val domainsFilter = Set("wasu.cloudcdn.net","kwmov.a.yximgs.com")

  def filter(log:String): Boolean ={
    // 获取域名
    try{
      val domainIndex1 = log.indexOf("://") + 3
      val domainIndex2 = log.indexOf("/", domainIndex1)
      val domain = log.substring(domainIndex1, domainIndex2)
      domainsFilter.contains(domain.trim())
    } catch {
      case e: Throwable => false
    }

  }
  def main(args: Array[String]) {
    val log1 = "@|@c01.i01@|@17@|@CNC_Taiyuan_108_157@|@CDNLOG_cache cnc-sx-221-204-202-073 @|@00000000@|@171.125" +
        ".81.236 61 0 [11/Jan/2017:16:08:30 +0800] \"GET http://wasu.cloudcdn.net/upic/2017/01/10/20/BMjAxNzAxMTAyMDA3MzVfMTI0NzkxOTUyXzE0ODk5Nzg0NDZfMV8z.mp4?tag=1-1484122003-h-0-vtic74t5zf-fa02dbd1a0c71842 HTTP/1.1\" 200 963384 \"-\" \"kwai-android\" mem 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000 61 1 - _ 171.125.81.236 - 963028 122.228.96.104";
    val log2 = "CDNLOG_cache cnc-he-060-008-151-247 61.181.5.162 5945.181 - [11/Jan/2017:16:08:48 +0800] \"GET http://kwmo.a.yximgs.com/upic/2017/01/10/20/BMjAxNzAxMTAyMDMyNDFfNDIzMjM4NTNfMTQ5MDExNTg0Nl8xXzM=.mp4?tag=1-1484122120-p-0-nu7iwwtlqs-732b91adc2567a22 HTTP/1.1\" 200 5823950 \"-\" \"kwai-android\" FCACHE_HIT_DISK 0.000 0.000 - - - - 0.000 5945.181 5945.181 1 - - 61.181.5.162 DISK HIT from 60.8.151.247 5823534 122.228.96.104";
    val log4 = "CDNLOG_cache cnc-he-060-008-151-247 61.181.5.162 5945.181 - [11/Jan/2017:16:08:48 +0800] \"GET " +
        "http://kwmov.a.yximgs.com/upic/2017/01/10/20/BMjAxNzAxMTAyMDMyNDFfNDIzMjM4NTNfMTQ5MDExNTg0Nl8xXzM=.mp4?tag=1-1484122120-p-0-nu7iwwtlqs-732b91adc2567a22 HTTP/1.1\" 200 5823950 \"-\" \"kwai-android\" FCACHE_HIT_DISK 0.000 0.000 - - - - 0.000 5945.181 5945.181 1 - - 61.181.5.162 DISK HIT from 60.8.151.247 5823534 122.228.96.104";

    val log3 = "@|@c01.i01@|@17@|@CNC_Taiyuan_108_157@|@CDNLOG_cache ctl-jx-059-063-188-151 @|@00000000@|@59.63.188.187" +
        " 1001 0 [11/Jan/2017:16:08:30 +0800] \"GET http://kwmo.a.yximgs" +
        ".com/upic/2017/01/10/20/BMjAxNzAxMTAyMDA3MzVfMTI0NzkxOTUyXzE0ODk5Nzg0NDZfMV8z.mp4?tag=1-1484122003-h-0-vtic74t5zf-fa02dbd1a0c71842 HTTP/1.1\" 200 963384 \"-\" \"kwai-android\" mem 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000 61 1 - _ 171.125.81.236 - 963028 59.63.188.151";
    val log5 = "@|@c01.i01@|@17@|@CNC_Taiyuan_108_157@|@CDNLOG_cache ctl-jx-059-063-188-151 @|@00000000@|@127.0.0" +
        ".1" +
        " 1001 0 [11/Jan/2017:16:08:30 +0800] \"GET http://kwmo.a.yximgs" +
        ".com/upic/2017/01/10/20/BMjAxNzAxMTAyMDA3MzVfMTI0NzkxOTUyXzE0ODk5Nzg0NDZfMV8z.mp4?tag=1-1484122003-h-0-vtic74t5zf-fa02dbd1a0c71842 HTTP/1.1\" 200 963384 \"-\" \"kwai-android\" mem 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000 61 1 - _ 171.125.81.236 - 963028 59.63.188.151";
    println(filter(log1))
    println(filter(log2))
    println(filter(""))
  }
}
