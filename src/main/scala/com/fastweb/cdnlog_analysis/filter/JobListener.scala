package com.fastweb.cdnlog_analysis.filter

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.scheduler._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by lfq on 2016/8/17.
  */
class JobListener(path: String) extends StreamingListener {
    val log = LoggerFactory.getLogger(JobListener.getClass)
    val format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

    override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
        val info = receiverStopped.receiverInfo
        val error = info.lastError
        val lastErrorMessage = info.lastErrorMessage
        val location = info.location
        val name = info.name
        log.warn("receiver stopped : receiver info " + error)
        log.warn("receiver stopped : lastErrorMessage " + lastErrorMessage)
        log.warn("receiver stopped : location " + location)
        log.warn("receiver stopped : name " + name)
    }

    override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
        val info = receiverError.receiverInfo
        val error = info.lastError
        val lastErrorMessage = info.lastErrorMessage
        val location = info.location
        val name = info.name
        log.error("receiverError : receiver info " + error)
        log.error("receiverError : lastErrorMessage " + lastErrorMessage)
        log.error("receiverError : location " + location)
        log.error("receiverError : name " + name)
    }

    /**
      *
      * @param batchCompleted
      * 将每个批次完成后的统计信息，保存到文件，文件以完成的时间（毫秒数）命名，
      */
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        val formats = new SimpleDateFormat("yyyy/MM/dd/HH/mm")
        val info = batchCompleted.batchInfo
        var numRecords = 0l
        val record = new ArrayBuffer[String]()
        info.receivedBlockInfo.foreach(i => {
            i._2.foreach(r => {
                numRecords += r.numRecords
                //log("receivedBlock" + " " + i._1 + " has " + r.numRecords + " records")
                record += (i._1 + "," + r.numRecords + "\n")
            })
        })
        if (numRecords == 0l) {
            return
        }
        val time = formats.format(new Date(info.batchTime.milliseconds))
        val file = path + File.separator + time
        val parent = new File(file).getAbsoluteFile.getParentFile
        if (!parent.exists()) {
            parent.mkdirs()
        }
        val writer = new PrintWriter(file)
        writer.write("blockId" + "," + "numRecords" + "\n")
        record.foreach(line => writer.write(line))
        writer.write("total," + numRecords + "\n")
        writer.close()
    }

    override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
        val info = receiverStarted.receiverInfo
        val error = info.lastError
        val lastErrorMessage = info.lastErrorMessage
        val location = info.location
        val name = info.name
        log.info("receiver started : receiver info " + error)
        log.info("receiver started : lastErrorMessage " + lastErrorMessage)
        log.info("receiver started : location " + location)
        log.info("receiver started : name " + name)
    }

}

object JobListener{
    def main(args: Array[String]) {
        val record = new ArrayBuffer[String]()
        record += "nihao"
        record += "2"
        record.foreach(println)
    }
}