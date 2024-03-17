package match8

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object part3 {
  def main(args: Array[String]): Unit = {
    val result = env.fromSource(getKafkaConf("ChangeRecord"), WatermarkStrategy.noWatermarks(), "part3")
      .map(e => {
        val arr = e.split(",")
        Change(arr(1), arr(3))
      })
      .filter(_.status == "预警")
      .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(3), Time.minutes(1)))
      .process(new ProcessAllWindowFunction[Change, (String, String), TimeWindow] {
        override def process(context: Context, elements: Iterable[Change], out: Collector[(String, String)]): Unit = {
          val value = elements.groupBy(_.id)
            .mapValues(_.size)
            .maxBy(_._2)
            ._2

          out.collect((DateFormatUtils.format(context.window.getEnd, "yyyy-MM-dd HH:mm:ss"), value.toString))
        }
      })

    result.print()
    result.addSink(new RedisSink[(String, String)](redisConf, new RedisMapper[(String, String)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "warning_last3min_everymin_out")

      override def getKeyFromData(data: (String, String)): String = data._1

      override def getValueFromData(data: (String, String)): String = data._2
    }))


    env.execute()
  }
}
