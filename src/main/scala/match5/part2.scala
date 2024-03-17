package match5

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.sql.PreparedStatement

object part2 {
  def main(args: Array[String]): Unit = {
    val result = env.fromSource(getKafkaConf("ChangeRecord"), WatermarkStrategy.noWatermarks(), "")
      .map(e => {
        val arr = e.split(",")
        change(arr(1), arr(3))
      })
      .keyBy(_.id)
      .process(new KeyedProcessFunction[String, change, (Int, String, Int, String)] {
        lazy val total: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("total", classOf[Int]))
        lazy val lastStatue: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastStatue", classOf[String]))
        //        lazy val lastMachineStatue: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastMachineStatue", classOf[String]))/

        override def processElement(value: change, ctx: KeyedProcessFunction[String, change, (Int, String, Int, String)]#Context, out: Collector[(Int, String, Int, String)]): Unit = {
          if (value.Status == "运行" && lastStatue.value() != "运行" && lastStatue.value() != null) {
            total.update(total.value() + 1)
            out.collect((ctx.getCurrentKey.toInt, lastStatue.value(), total.value(), DateFormatUtils.format(ctx.timestamp(), "yyyy-MM-dd HH:mm:ss")))
          }
          lastStatue.update(value.Status)
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
      jdbcExecutionOptions,
      mysqlConf
    ))


    env.execute()
  }
}
