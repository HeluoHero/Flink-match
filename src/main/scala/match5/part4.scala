package match5

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

object part4 {
  def main(args: Array[String]): Unit = {
    // 第三题的另外一种解法
    val result = env.fromSource(getKafkaConf("ChangeRecord"), WatermarkStrategy.noWatermarks(), "")
      .map(e => {
        val arr = e.split(",")
        change(arr(1), arr(3))
      })
      .filter(_.Status == "预警")
      .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(3), Time.minutes(1)))
      .process(new ProcessAllWindowFunction[change, (String, String), TimeWindow] {
        override def process(context: Context, elements: Iterable[change], out: Collector[(String, String)]): Unit = {
          val id = elements.groupBy(_.id)
            .mapValues(e => e.size)
            .maxBy(_._2)
            ._1

          out.collect((id, DateFormatUtils.format(context.window.getEnd, "yyyy-MM-dd HH:mm:ss")))
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
