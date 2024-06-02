package com.heluo.match4

import com.heluo.match4.comment.BaseApp
import com.heluo.match4.function.CleanFlatMapFunction
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Part3_2 extends BaseApp{
  def main(args: Array[String]): Unit = {
    Part3_2.start(10012, 4, "part3", "ChangeRecord")
  }

  override def handle(env: StreamExecutionEnvironment, stream: DataStream[String]): Unit = {
    stream.flatMap(new CleanFlatMapFunction)
      .filter(e => e.status == "预警" && e.check % 2 == 0)
      .keyBy(_.id)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(3L)))
      .process(new ProcessWindowFunction[ChangeRecord, (String, Int, String), String,TimeWindow]() {
        override def process(key: String, context: Context, elements: Iterable[ChangeRecord], out: Collector[(String, Int, String)]): Unit = {
          out.collect((key, elements.size, DateFormatUtils.format(context.window.getEnd, "yyyy-MM-dd HH:mm:ss")))
        }
      }).print()


  }
}
