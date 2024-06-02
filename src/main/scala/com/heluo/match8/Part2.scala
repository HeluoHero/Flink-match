package com.heluo.match8

import com.heluo.match8.comment.BaseApp
import com.heluo.match8.function.ChangeRecordFlatMap
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.PreparedStatement

object Part2 extends BaseApp {
  def main(args: Array[String]): Unit = {
    Part2.start("ChangeRecord", "ChangeRecord")
  }

  override def handle(stream: DataStream[String]): Unit = {
    val result2 = stream.flatMap(new ChangeRecordFlatMap)
      .filter(e => e.status.equals("预警") && e.check % 2 == 0)
      .keyBy(_.id)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(3L)))
      .process(new ProcessWindowFunction[ChangeRecord, (String, Int, String), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[ChangeRecord], out: Collector[(String, Int, String)]): Unit = {

          out.collect(elements.head.id, elements.size, DateFormatUtils.format(context.window.getEnd, "yyyy-MM-dd HH:MM:ss"))
        }
      })
    result2.print()

    result2.addSink(JdbcSink.sink(
      "insert into threemin_warning_state_agg values(?, ?, ?)",
      new JdbcStatementBuilder[(String, Int, String)] {
        override def accept(t: PreparedStatement, u: (String, Int, String)): Unit = {
          t.setString(1, u._1)
          t.setString(2, u._2.toString)
          t.setString(3, u._3)
        }
      },
      jdbcExecutionOptions,
      mysqlConf))

  }
}
