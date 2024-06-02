package com.heluo.match4.function

import com.heluo.match4.ChangeRecord
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat


class CleanFlatMapFunction extends RichFlatMapFunction[String, ChangeRecord] {
  override def flatMap(value: String, out: Collector[ChangeRecord]): Unit = {
    try {
      val arr = value.split(",")
      out.collect(ChangeRecord(
        arr(1),
        arr(3),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(4)).getTime,
        arr.last.toInt))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(s"清洗不正常数据: $value")
    }
  }
}
