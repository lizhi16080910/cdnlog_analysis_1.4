package org.apache.spark.streaming.kafka.filter

import java.util.Properties

import kafka.consumer.{ Consumer, ConsumerConfig, ConsumerConnector, KafkaStream }
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.Utils

import scala.collection.Map
import scala.reflect._

/**
 * Created by lfq on 2016/7/25.
 */

class FilterKafkaReceiver[K: ClassTag, V: ClassTag, U <: Decoder[_]: ClassTag, T <: Decoder[_]: ClassTag](
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
          store((msgAndMetadata.key, msgAndMetadata.message()))
        }
      } catch {
        case e: Throwable => logError("Error handling message; exiting", e)
      }
    }
  }
}
