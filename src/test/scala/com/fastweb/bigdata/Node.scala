package com.fastweb.bigdata

/**
  * Created by lfq on 2017/4/12.
  */
class Node(val start: Long, val end: Long, val code: Int) extends
Comparable[Node] {
    override def compareTo(o: Node): Int = {
        if (o.start > this.start) 1
        else {
            if (o.start < this.start) -1 else 0
        }
    }
    override def toString = s"Node($start, $end, $code)"
}
