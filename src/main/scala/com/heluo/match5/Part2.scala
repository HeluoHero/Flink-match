package com.heluo.match5

import com.heluo.match5.comment.BaseApp
import com.heluo.match5.function.ChangeRecordFlatMapFunction
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.Collector

import java.sql.PreparedStatement

object Part2 extends BaseApp {
  def main(args: Array[String]): Unit = {
    Part2.start(10012, 4, "aaaa", "ChangeRecord")
  }

  override def handle(stream: DataStream[String]): Unit = {
    stream.flatMap(new ChangeRecordFlatMapFunction)
      .keyBy(_.id)
      .process(new KeyedProcessFunction[String, ChangeRecord, (Int, String, Int, String)] {
        lazy val lastStatusState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastStatus", classOf[String]))
        lazy val countState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("countState", classOf[Int]))

        override def processElement(value: ChangeRecord, ctx: KeyedProcessFunction[String, ChangeRecord, (Int, String, Int, String)]#Context, out: Collector[(Int, String, Int, String)]): Unit = {
          val lastStatus = lastStatusState.value()
          val nowStatus = value.status
          if (lastStatus != "运行" && nowStatus == "运行" && lastStatus != null) {
            countState.update(countState.value() + 1)
            out.collect((ctx.getCurrentKey.toInt, lastStatus, countState.value(), DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")))
          }
          lastStatusState.update(value.status)
        }
      }).addSink(JdbcSink.sink[(Int, String, Int, String)](
      "insert into change_state_other_to_run_agg values(?, ?, ?, ?)",
      new JdbcStatementBuilder[(Int, String, Int, String)] {
        override def accept(t: PreparedStatement, u: (Int, String, Int, String)): Unit = {
          t.setString(1, u._1.toString)
          t.setString(2, u._2)
          t.setString(3, u._3.toString)
          t.setString(4, u._4)
        }
      },
      jdbcExecutionOptions,
      mysqlConf
    ))

  }
}
