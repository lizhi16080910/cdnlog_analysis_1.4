package com.fastweb.cdnlog_analysis.slowrate

import java.io.FileInputStream
import java.util.{Collections, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
/**
  * Created by lfq on 2016/11/21.
  */
object SparkJob {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setMaster("local[4]")
            sparkConf.setAppName("test")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.textFile("F:\\业务资料\\快手慢速比计算\\part-00000")
        val rdd2= rdd.map(line => SlowRateLogInfo(line))
        .map(x => {
            x match {
                case Left(msg) => msg
                case Right(result) => null
            }
        }).filter(x => x != null)

        val rdd3 = rdd2.map( x => {
            val key = (x.domain, x.host, x.isp, x.province, x.time, x.ifSlow)
            val value = 1l
            (key, value)
        } ).reduceByKey(_+_).foreach(println)
        sc.stop()

    }

    def getJobTime(): String = {
        val offsetDir = "/user/camus-test/exec/offset"
        val fs = FileSystem.get(new Configuration())
        val offsetDirP: Path = new Path(offsetDir)

        val fsts = fs.listStatus(offsetDirP)
        if (fsts.length == 0) {
            return ""
        }
        val dates = new ArrayBuffer[String]
        fsts.foreach(file => dates += file.getPath.getName)
        Collections.sort(dates)
        dates.get(0)
    }

    def loadProperty(path: String, sparkConf: SparkConf): Unit = {
        val props = new Properties()
        val inputStream = new FileInputStream(path)
        props.load(inputStream)
        inputStream.close()
        props.keySet().foreach(key => {
            sparkConf.set(key.asInstanceOf[String], props.getProperty(key.asInstanceOf[String]))
        })
    }

}
