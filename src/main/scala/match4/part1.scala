package match4

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector


// 3,117,0006,2024-03-06 10:01:24,2024-03-06 10:01:26,2024-03-06 10:01:32,25832,1900-01-01 00:00:00,180057,0
object part1 {
  def main(args: Array[String]): Unit = {
    env.setParallelism(1)

    env.fromSource(getKafkaConf("ProduceRecord"), WatermarkStrategy.noWatermarks(), "")
      .map(e => {
        val arr = e.split(",")
        ProduceRecord(arr(1), arr.last)
      })
      .keyBy(_.produceID)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
      .process(new ProcessWindowFunction[ProduceRecord, (String, Int), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[ProduceRecord], out: Collector[(String, Int)]): Unit = {
          out.collect((key, elements.count(_.changeHandleState == "1")))
        }
      })
      .addSink(new RedisSink[(String, Int)](redisConf, new RedisMapper[(String, Int)] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "totalproduce")

        override def getKeyFromData(data: (String, Int)): String = data._1

        override def getValueFromData(data: (String, Int)): String = data._2.toString
      }))

    env.execute()
  }
}
