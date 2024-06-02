package com.heluo.match4

import com.heluo.match4.comment.BaseApp
import com.heluo.match4.function.CleanFlatMapFunction
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Part2 extends BaseApp {
  def main(args: Array[String]): Unit = {
    Part2.start(10011, 1, "part2", "ChangeRecord")
  }

  override def handle(env: StreamExecutionEnvironment, stream: DataStream[String]): Unit = {
    stream.flatMap(new CleanFlatMapFunction)
      .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner[ChangeRecord] {
          override def extractTimestamp(element: ChangeRecord, recordTimestamp: Long): Long = element.changeStartTime
        }))
      .keyBy(_.id)
      .process(new KeyedProcessFunction[String, ChangeRecord, (String, String)] {
        lazy val lastStatusState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastStatus", classOf[String]))
        lazy val changeStartTime: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("changeStartTime", classOf[String]))
        lazy val registerTimeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("registerTimeState", classOf[Long]))
        lazy val registerList: ListState[Long] = getRuntimeContext.getListState(new ListStateDescriptor[Long]("", classOf[Long]))

        override def processElement(value: ChangeRecord, ctx: KeyedProcessFunction[String, ChangeRecord, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
          try {
            val lastStatus: String = lastStatusState.value()
            val nowStatus: String = value.status
            if (nowStatus == "预警") {
              if (lastStatus == null) {
                registerTimeState.update(value.changeStartTime + 30000L)
                ctx.timerService().registerEventTimeTimer(value.changeStartTime + 30000L)
                //                println(s"注册定时器 ${ctx.getCurrentKey} 当前水位线为："+""+ctx.timerService().currentWatermark())
                registerList.add(ctx.timerService().currentWatermark())
              }
              lastStatusState.update(value.status)
              changeStartTime.update(DateFormatUtils.format(value.changeStartTime, "yyyy-MM-dd HH:mm:ss"))
            } else if (nowStatus != "预警" && lastStatus != null) {
              println(s"删除定时器: ${ctx.getCurrentKey}")
              ctx.timerService().deleteEventTimeTimer(registerTimeState.value())
              lastStatusState.clear()
              changeStartTime.clear()
              registerList.clear()
            }
          } catch {
            case _: Exception => println(s"处理数据：${value}")
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, ChangeRecord, (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
          out.collect(ctx.getCurrentKey, s"${changeStartTime.value()}:设备${ctx.getCurrentKey}连续30秒为预警状态请尽快处理！")
          val list: ListBuffer[Long] = ListBuffer[Long]()
          registerList.get().forEach(e => {
            list.append(e)
          })

          println("-" * 20)
          println(s"最小注册时间${DateFormatUtils.format(list.min, "yyyy-MM-dd HH:mm:ss.SSS")}")
          println(s"触发计算器时间戳： ${DateFormatUtils.format(timestamp, "yyyy-MM-dd HH:mm:ss.SSS")}")
          println(s"触发定时器${ctx.getCurrentKey} 当前水位线为：" + "" + DateFormatUtils.format(ctx.timerService().currentWatermark(), "yyyy-MM-dd HH:mm:ss.SSS"))
          println("=" * 20)
          changeStartTime.clear()
          lastStatusState.clear()
          registerList.clear()
        }
      })
    /*.addSink(new RedisSink[(String, String)](redisConf, new RedisMapper[(String, String)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "warning30sMachine")

      override def getKeyFromData(data: (String, String)): String = data._1

      override def getValueFromData(data: (String, String)): String = data._2
    }))*/

  }
}
