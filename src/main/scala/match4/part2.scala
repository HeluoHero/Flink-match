package match4

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration

object part2 {
  def main(args: Array[String]): Unit = {
    // 26_254_623,112,25,运行,2024-03-06 10:00:55,2024-03-06 10:01:26,29

    env.fromSource(getKafkaConf("ChangeRecord"), WatermarkStrategy.noWatermarks(), "part2")
      .map(e => {
        val arr = e.split(",")
        ChangeRecord(arr(1), arr(3), arr(4), arr.last.toInt)
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(2))
          .withTimestampAssigner(new SerializableTimestampAssigner[ChangeRecord]() {
            override def extractTimestamp(element: ChangeRecord, recordTimestamp: Long): Long = {
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.change_start_time).getTime
            }
          }))
      .keyBy(_.id)
      .process(new KeyedProcessFunction[String, ChangeRecord, (String, String)] {
        lazy val lastStatue: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastStatue", classOf[String]))
        lazy val changeStartTime: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("changeStartTime", classOf[String]))

        override def processElement(value: ChangeRecord, ctx: KeyedProcessFunction[String, ChangeRecord, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
          if (value.status == "预警"){
            if (lastStatue.value() == null) {
              ctx.timerService().registerEventTimeTimer(30000L)
              changeStartTime.update(value.change_start_time)
            } else {
              changeStartTime.update(value.change_start_time)
            }
          } else if (value.status != "预警" && lastStatue.value() != null) {
            ctx.timerService().deleteEventTimeTimer(30000L)
          }

          lastStatue.update(value.status)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, ChangeRecord, (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
          out.collect(ctx.getCurrentKey, changeStartTime.value() + s":设备${ctx.getCurrentKey}连续30秒为预警状态请尽快处理！")
          lastStatue.clear()
        }
      }).addSink(new RedisSink[(String, String)](redisConf, new RedisMapper[(String, String)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "warning30sMachine")

      override def getKeyFromData(data: (String, String)): String = data._1

      override def getValueFromData(data: (String, String)): String = data._2
    }))


    env.execute()
  }
}

/*
.keyBy(_.id)
.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
.process(new ProcessWindowFunction[ChangeRecord, Long, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[ChangeRecord], out: Collector[Long]): Unit = {
    val list = elements.toList
      .sortBy(e => new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(e.change_start_time).getTime)
      .reverse
    out.collect(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(list.head.change_start_time).getTime - new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(list.last.change_start_time).getTime)
  }
})
.print()
*/
