package com.heluo.match8

import com.heluo.match8.comment.BaseApp
import com.heluo.match8.function.ChangeRecordFlatMap
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.Collector

import java.sql.PreparedStatement

object Part3 extends BaseApp {
  def main(args: Array[String]): Unit = {
    Part3.start("change_record", "ChangeRecord")
  }

  override def handle(stream: DataStream[String]): Unit = {
    val result3 = stream.flatMap(new ChangeRecordFlatMap)
      .keyBy(_.id)
      .process(new KeyedProcessFunction[String, ChangeRecord, (Int, String, Int, String)] {

        lazy val lastMachineStateStatus: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("last_machineS_state", classOf[String]))
        lazy val totalChangeTorunState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("total_change_torun", classOf[Int]))

        override def processElement(value: ChangeRecord, ctx: KeyedProcessFunction[String, ChangeRecord, (Int, String, Int, String)]#Context, out: Collector[(Int, String, Int, String)]): Unit = {
          val lastMachineState = lastMachineStateStatus.value()
          val nowMachineState = value.status
          val totalChangeTorun: Int = totalChangeTorunState.value()
          if (lastMachineState != null && !lastMachineState.equals("运行") && nowMachineState.equals("运行")) {
            val count = totalChangeTorun + 1
            totalChangeTorunState.update(count)
            out.collect(ctx.getCurrentKey.toInt, lastMachineState, count, DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"))
          }
          lastMachineStateStatus.update(nowMachineState)
        }
      })

    result3.print()
    result3.addSink(JdbcSink.sink(
      "insert into change_state_other_to_run_agg value(?, ?, ?, ?)",
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
