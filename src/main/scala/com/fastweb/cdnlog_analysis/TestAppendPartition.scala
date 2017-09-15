package com.fastweb.cdnlog_analysis

import java.util.ArrayList

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

object TestAppendPartition {
  def main(args: Array[String]) {
    val hiveConf = new HiveConf()
    hiveConf.set("hive.metastore.uris", "thrift://115.238.146.2:9083")
    val hiveClient = new HiveMetaStoreClient(hiveConf)
    try {
      //var table = hiveClient.getTable("default", "cdn_log_test")
      //println(table)
      //var partition = hiveClient.getPartition("default", "cdn_log_test", "timestmp=1427787120000")
      //println(partition)

      // var values = new ArrayList[String]()
      //values.add("2015040118")
      // partition.setValues(values)
      //hiveClient.add_partition(partition)
      var month = 201504
      for (day <- 17 to 30) {
        for (hour <- 0 to 24) {
          for (minute <- 0 to 60) {
            var partitions = new ArrayList[String]()
            partitions.add("" + month)
            partitions.add("" + day)
            partitions.add("" + hour)
            partitions.add("" + minute)
            hiveClient.appendPartition("default", "testpartition", partitions)
          }
        }
      }

      //hiveClient.deletePartitionColumnStatistics("default", "cdn_log", "minute_=201503", "minute_")
      //      def dropPartition = (tableName: String, partition: String) => {
      //        try {
      //          hiveClient.dropPartition("default", tableName, partition, false)
      //        } catch {
      //          case e: Throwable => {}
      //        }
      //
      //      }
      //      for (hour <- 0.to(16)) {
      //        if (hour < 10) {
      //          for (minute <- 0 to 60) {
      //            if (minute < 10) {
      //              dropPartition("cdnlog", "month_=201504/day_=14/hour_=0" + hour + "/minute_=0" + minute)
      //            } else {
      //              dropPartition("cdnlog", "month_=201504/day_=14/hour_=0" + hour + "/minute_=" + minute)
      //            }
      //          }
      //        } else {
      //          for (minute <- 0 to 60) {
      //            if (minute < 10) {
      //              dropPartition("cdnlog", "month_=201504/day_=14/hour_=" + hour + "/minute_=0" + minute)
      //            } else {
      //              dropPartition("cdnlog", "month_=201504/day_=14/hour_=" + hour + "/minute_=" + minute)
      //            }
      //          }
      //        }
      //      }

      var partitionNames = hiveClient.listPartitionNames("default", "testpartition", Short.MaxValue)
      println(partitionNames.size())
    } finally {
      hiveClient.close()
    }
  }
}