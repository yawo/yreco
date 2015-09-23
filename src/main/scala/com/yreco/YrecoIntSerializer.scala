package com.yreco

import edu.berkeley.cs.amplab.spark.indexedrdd.KeySerializer

/**
 * Created by yawo on 23/09/15.
 */

class YrecoIntSerializer extends KeySerializer[Int] {
  override def toBytes(k: Int) = Array(
    ((k >> 24) & 0xFF).toByte,
    ((k >> 16) & 0xFF).toByte,
    ((k >>  8) & 0xFF).toByte,
    ( k        & 0xFF).toByte)

  override def fromBytes(b: Array[Byte]): Int =
      (b(4).toInt << 24) & (0xFF << 24) |
      (b(5).toInt << 16) & (0xFF << 16) |
      (b(6).toInt <<  8) & (0xFF <<  8) |
      b(7).toInt         &  0xFF
}

object YrecoIntSerializer{
  implicit val intSer = new YrecoIntSerializer
}
