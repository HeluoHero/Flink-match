package com.heluo.match4

import com.heluo.match4.comment.BaseApp
import com.heluo.match4.function.CleanFlatMapFunction
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.scala._
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.PreparedStatement

object Part3 extends BaseApp {
  def main(args: Array[String]): Unit = {
    Part3.start(10012, 4, "part3", "ChangeRecord")
  }

  override def handle(env: StreamExecutionEnvironment, stream: DataStream[String]): Unit = {
    stream.flatMap(new CleanFlatMapFunction)
      .filter(e => e.status == "预警" && e.check % 2 == 0)
      .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(3L)))
      .process(new ProcessAllWindowFunction[ChangeRecord, (String, Int, String), TimeWindow] {
        override def process(context: Context, elements: Iterable[ChangeRecord], out: Collector[(String, Int, String)]): Unit = {
          elements.groupBy(_.id)
            .mapValues(_.size)
            .foreach {
              case (k, v) => out.collect((k, v, DateFormatUtils.format(context.window.getEnd, "yyyy-MM-dd HH:mm:ss")))
            }
        }
      })
      .addSink(JdbcSink.sink(
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
