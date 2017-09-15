package org.apache.spark.streaming.kafka.filter

/**
  * Created by lfq on 2016/8/8.
  */
class ReceiverFilter extends Filter{
    override def filter(log: String): Boolean = true
}
