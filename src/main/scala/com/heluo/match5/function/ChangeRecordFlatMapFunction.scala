package com.heluo.match5.function

import com.heluo.match5.ChangeRecord
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class ChangeRecordFlatMapFunction extends FlatMapFunction[String, ChangeRecord] {
  override def flatMap(value: String, out: Collector[ChangeRecord]): Unit = {
    try {
      val arr = value.split(",")
      out.collect(ChangeRecord(arr(1), arr(3), arr.last.toInt))
    } catch {
      case _: Exception => println(s"过滤不正常数据：${value}")
    }
  }
}
