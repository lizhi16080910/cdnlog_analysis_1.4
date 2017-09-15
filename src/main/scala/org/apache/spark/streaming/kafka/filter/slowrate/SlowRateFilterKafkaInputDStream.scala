/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.kafka.filter.slowrate

import kafka.serializer.Decoder
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.filter.Filter
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.Map
import scala.reflect.ClassTag

/**
  * Input stream that pulls messages from a Kafka Broker.
  *
  * @param kafkaParams Map of kafka configuration parameters.
  *                    See: http://kafka.apache.org/configuration.html
  * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
  * in its own thread.
  * @param storageLevel RDD storage level.
  */
private[streaming]
class SlowRateFilterKafkaInputDStream[
K: ClassTag,
V: ClassTag,
U <: Decoder[_]: ClassTag,
T <: Decoder[_]: ClassTag](
                              @transient ssc_ : StreamingContext,
                              kafkaParams: Map[String, String],
                              topics: Map[String, Int],
                              useReliableReceiver: Boolean,
                              storageLevel: StorageLevel ,
                              filterClass: Class[_ <: Filter]
                          ) extends ReceiverInputDStream[(K, V)](ssc_) with Logging {

    def getReceiver(): Receiver[(K, V)] = {
        if (!useReliableReceiver) {
            //添加了过滤函数的Receiver
            new SlowRateFilterKafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel,filterClass)
        } else {
            //添加了过滤函数的Receiver
            new SlowRateFilterReliableKafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel,filterClass)
        }
    }
}


