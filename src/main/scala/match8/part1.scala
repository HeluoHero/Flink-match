package match8

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

object part1 {
  def main(args: Array[String]): Unit = {

    // 3,115,401,66,68,39.62,52.06,12,22,16,2024-03-12 10:31:57
    val result = env.fromSource(getKafkaConf("EnvironmentData"), WatermarkStrategy.noWatermarks(), "part1")
      .map(e => {
        val arr = e.split(",")
        Environment(arr(1), arr(5).toDouble, arr.last)
      })
      .keyBy(_.id)
      .process(new KeyedProcessFunction[String, Environment, (String, String)] {

        lazy val lastDegree: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastDegree", classOf[Double]))
        lazy val endTime: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("endTime", classOf[String]))

        override def processElement(value: Environment, ctx: KeyedProcessFunction[String, Environment, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
          if (value.degree >= 38.0) {
            if (lastDegree.value() == null) ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 3 * 60 * 1000L)
            lastDegree.update(value.degree)
            endTime.update(value.create_time)
          } else {
            if (lastDegree.value() != null) ctx.timerService().deleteProcessingTimeTimer(3 * 60 * 1000L)
            lastDegree.clear()
            endTime.clear()
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Environment, (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
          super.onTimer(timestamp, ctx, out)
          out.collect((ctx.getCurrentKey) + "-" + DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"), s"设备${ctx.getCurrentKey}连续三分钟温度高于38度请及时处理！")
          lastDegree.clear()
          endTime.clear()
        }

      })

    result.print()
    result
      .addSink(new RedisSink[(String, String)](redisConf, new RedisMapper[(String, String)] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "env_temperature_monitor")

        override def getKeyFromData(data: (String, String)): String = data._1

        override def getValueFromData(data: (String, String)): String = data._2
      }))

    env.execute()
  }
}
