package com.heluo.match4

import com.heluo.match4.comment.BaseApp
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Part1_3 extends BaseApp {
  def main(args: Array[String]): Unit = {
    Part1_3.start(10010, 4, "part1", "ProduceRecord")
  }

  override def handle(env: StreamExecutionEnvironment, stream: DataStream[String]): Unit = {
    stream.flatMap(new FlatMapFunction[String, ProduceRecord] {
      override def flatMap(value: String, out: Collector[ProduceRecord]): Unit = {
        val arr = value.split(",")
        out.collect(ProduceRecord(arr(1), arr.last))
      }
    })
      .filter(_.status == "1")
      .keyBy(_.id)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(5L)))
      .process(new ProcessWindowFunction[ProduceRecord, (String, Int), String, TimeWindow]() {
        override def process(key: String, context: Context, elements: Iterable[ProduceRecord], out: Collector[(String, Int)]): Unit = {
          out.collect((key, elements.size))
        }
      }).print()
  }
}
