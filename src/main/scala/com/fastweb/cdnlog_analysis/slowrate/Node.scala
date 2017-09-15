package com.fastweb.cdnlog_analysis.slowrate

/**
  * Created by lfq on 2017/1/12.
  */
class Node(val start:Long, val end:Long, val isp:Int, val ispName:String, val prv:Int, val prvName:String) extends
Comparable[Node]{
    override def compareTo(o: Node): Int = {
        if(o.start > this.start) 1 else {if(o.start < this.start) -1 else 0 }
    }

    override def toString = s"Node(start=$start, end=$end, isp=$isp, ispName=$ispName, prv=$prv, prvName=$prvName)"
}
