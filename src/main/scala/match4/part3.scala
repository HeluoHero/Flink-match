package match4

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.time.Duration

object part3 {
  def main(args: Array[String]): Unit = {
    env.fromSource(getKafkaConf("ChangeRecord"), WatermarkStrategy.noWatermarks(), "part3")
      .map(e => {
        val arr = e.split(",")
        ChangeRecord(arr(1), arr(3), arr(4), arr.last.toInt)
      })
      .filter(e => e.status == "预警" && e.check % 2 == 0)
      .keyBy(_.id)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(3)))
      .process(new ProcessWindowFunction[ChangeRecord, (Int, Int, String), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[ChangeRecord], out: Collector[(Int, Int, String)]): Unit = {
          out.collect((key.toInt, elements.size, DateFormatUtils.format(context.window.getEnd, "yyyy-MM-dd HH:mm:ss")))
        }
      }).addSink(JdbcSink.sink("insert into threemin_warning_state_agg values(?, ?, ?)",
      new JdbcStatementBuilder[(Int, Int, String)] {
        override def accept(t: PreparedStatement, u: (Int, Int, String)): Unit = {
          t.setString(1, u._1.toString)
          t.setString(2, u._2.toString)
          t.setString(3, u._3)
        }
      },
      jdbcExecutionOptions,
      mysqlSink))

    env.execute()
  }
}
