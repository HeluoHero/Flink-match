package match8

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}

import java.sql.PreparedStatement

object part4 {
  def main(args: Array[String]): Unit = {
    val result = env.fromSource(getKafkaConf("ChangeRecord"), WatermarkStrategy.noWatermarks(), "part4")
      .map(e => {
        val arr = e.split(",")
        Change(arr(1), arr(3))
      })
      .keyBy(_.id)
      .process(new KeyedProcessFunction[String, Change, (Int, String, Int, String)] {
        lazy val lastStatus: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastStatus", classOf[String]))
        lazy val countState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("countState", classOf[Int]))

        override def processElement(value: Change, ctx: KeyedProcessFunction[String, Change, (Int, String, Int, String)]#Context, out: Collector[(Int, String, Int, String)]): Unit = {
          if (value.status == "运行" && lastStatus.value() != "运行" && lastStatus.value() != null) {
            countState.update(countState.value() + 1)
            out.collect((ctx.getCurrentKey.toInt, lastStatus.value(), countState.value(), DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")))
          }
          lastStatus.update(value.status)
        }
      })

    result.print()
    result.addSink(JdbcSink.sink("insert into change_state_other_to_run_agg values(?, ?, ?, ?)",
      new JdbcStatementBuilder[(Int, String, Int, String)] {
        override def accept(t: PreparedStatement, u: (Int, String, Int, String)): Unit = {
          t.setString(1, u._1.toString)
          t.setString(2, u._2)
          t.setString(3, u._3.toString)
          t.setString(4, u._4)
        }
      },
      jdbcExceptionOptions,
      mysqlConf))


    env.execute()
  }
}
