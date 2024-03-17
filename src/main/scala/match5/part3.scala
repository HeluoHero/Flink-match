package match5

import org.apache.commons.lang.time.{DateFormatUtils, DateUtils}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable

object part3 {
  def main(args: Array[String]): Unit = {
    val resultZ = env.fromSource(getKafkaConf("ChangeRecord"), WatermarkStrategy.noWatermarks(), "")
      .map(e => {
        val arr = e.split(",")
        change(arr(1), arr(3))
      })
      .filter(_.Status == "预警")
      .keyBy(_.id)
      .window(SlidingProcessingTimeWindows.of(Time.minutes(3), Time.minutes(1)))
      .process(new ProcessWindowFunction[change, (String, Int, Long), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[change], out: Collector[(String, Int, Long)]): Unit = {
          out.collect((key, elements.size, context.window.getEnd))
        }
      })
      .keyBy(_._3)
      .process(new KeyedProcessFunction[Long, (String, Int, Long), (String, String)] {
        val map: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()

        override def processElement(value: (String, Int, Long), ctx: KeyedProcessFunction[Long, (String, Int, Long), (String, String)]#Context, out: Collector[(String, String)]): Unit = {
          if (map.isEmpty) ctx.timerService().registerProcessingTimeTimer(value._3 + 1000L)
          map.put(value._1, value._2)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, (String, Int, Long), (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
          super.onTimer(timestamp, ctx, out)
          val result: (String, Int) = map.toList.sortBy(_._2).reverse.head
          out.collect(DateFormatUtils.format(ctx.getCurrentKey, "yyyy-MM-dd HH:mm:ss"), result._1)
          map.clear()
        }
      })
    resultZ.print()

    resultZ.addSink(new RedisSink[(String, String)](redisConf, new RedisMapper[(String, String)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "warning_last3min_everymin_out")

      override def getKeyFromData(data: (String, String)): String = data._1

      override def getValueFromData(data: (String, String)): String = data._2
    }))


    env.execute()
  }
}
