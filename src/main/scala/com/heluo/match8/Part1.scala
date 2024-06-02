package com.heluo.match8

import com.heluo.match8.comment.BaseApp
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

object Part1 extends BaseApp {

  def main(args: Array[String]): Unit = {
    Part1.start("change_record", "EnvironmentData")
  }

  override def handle(stream: DataStream[String]): Unit = {
    val result1 = stream.flatMap(new FlatMapFunction[String, EnvironmentData] {
      override def flatMap(value: String, out: Collector[EnvironmentData]): Unit = {
        try {
          val arr = value.split(",")
          out.collect(EnvironmentData(arr(1), arr(5).toDouble))
        } catch {
          case e: Exception => println(s"过滤不正常数据：${value}")
        }
      }
    }).keyBy(_.id)
      .process(new KeyedProcessFunction[String, EnvironmentData, (String, String)] {

        lazy val lastTemperatureState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temperature_state", classOf[Double]))
        lazy val lastTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last_timer_state", classOf[Long]))

        override def processElement(value: EnvironmentData, ctx: KeyedProcessFunction[String, EnvironmentData, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
          val lastTemperature = lastTemperatureState.value()
          val nowTemperature = value.temperature

          if (lastTemperature == 0.0 && nowTemperature > 38.0) {
            val time = System.currentTimeMillis() + (3 * 60 * 1000L)
            println(s"${ctx.getCurrentKey}注册定时器：$time")
            ctx.timerService().registerProcessingTimeTimer(time)
            lastTemperatureState.update(nowTemperature)
            lastTimerState.update(time)
          } else if (lastTemperature != 0.0) {
            if (nowTemperature <= 38.0) {
              println(s"${ctx.getCurrentKey}删除定时器：$lastTimerState")
              ctx.timerService().deleteProcessingTimeTimer(lastTimerState.value())
              lastTemperatureState.clear()
              lastTimerState.clear()
            } else {
              lastTemperatureState.update(nowTemperature)
            }
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, EnvironmentData, (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
          out.collect(ctx.getCurrentKey, DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"))
          lastTemperatureState.clear()
          lastTimerState.clear()

        }
      })
    result1.print()

    result1.addSink(new RichSinkFunction[(String, String)] {
      var conn: Connection = _
      val namespace: String = "gyflinkresult"
      val tableName: String = "EnvTemperatureMonitor"
      val columnFamily: String = "info"

      override def open(parameters: Configuration): Unit = {
        val conf = HBaseConfiguration.create()
        conf.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1,bigdata2,bigdata3")
        conn = ConnectionFactory.createConnection(conf)
      }

      override def invoke(value: (String, String), context: SinkFunction.Context): Unit = {
        val table = conn.getTable(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tableName)))

        val put = new Put(Bytes.toBytes(value._1 + "-" + value._2))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("machine_id"), Bytes.toBytes(value._1))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("out_warning_time"), Bytes.toBytes(value._2))

        table.put(put)

        if (table != null) table.close()
      }

      override def close(): Unit = {
        conn.close()
      }
    })


  }
}
