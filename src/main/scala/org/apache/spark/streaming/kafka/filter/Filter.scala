package org.apache.spark.streaming.kafka.filter


/**
  * Created by lfq on 2016/7/25.
  */
trait Filter{
    def filter(log:String): Boolean
}
