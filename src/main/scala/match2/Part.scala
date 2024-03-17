package match2

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

case class Order(id: String, status: Int)

object Part {
  def main(args: Array[String]): Unit = {
    val result2 = OutputTag[Int]("refundcountminute")
    val result3 = OutputTag[String]("cancelrate")

    val result1 = env.fromSource(kafkaConf, WatermarkStrategy.noWatermarks(), "")
      .map(e => {
        val str = e.replaceAll("\"", "")
        val arr = str.split(",")
        Order(arr.head, arr(4).toInt)
      })
      .keyBy(_ => true)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      .process(new ProcessWindowFunction[Order, Int, Boolean, TimeWindow] {
        var totalcount = 0
        var refundcountminute = 0
        var cancel = 0
        var total = 0

        override def process(key: Boolean, context: Context, elements: Iterable[Order], out: Collector[Int]): Unit = {
          elements.foreach(value => {
            total += 1
            if (value.status == 1001 || value.status == 1002 || value.status == 1004) totalcount += 1
            else if (value.status == 1005) refundcountminute += 1
            else if (value.status == 1003) cancel += 1
          })
          out.collect(totalcount)
          context.output(result2, refundcountminute)
          val cancelrate = f"${cancel * 1D / total * 100}%.1f" + "%"
          context.output(result3, cancelrate)
          totalcount = 0
          refundcountminute = 0
          cancel = 0
          total = 0
        }
      })

    result1.addSink(new RedisSink[Int](redisConf, new RedisMapper[Int] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET, "totalcount")

      override def getKeyFromData(data: Int): String = "totalcount"

      override def getValueFromData(data: Int): String = data.toString
    }))

    result1.getSideOutput(result2).addSink(new RedisSink[Int](redisConf, new RedisMapper[Int] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET, "refundcountminute")

      override def getKeyFromData(data: Int): String = "refundcountminute"

      override def getValueFromData(data: Int): String = data.toString
    }))

    result1.getSideOutput(result3).addSink(new RedisSink[String](redisConf, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET, "cancelrate")

      override def getKeyFromData(data: String): String = "cancelrate"

      override def getValueFromData(data: String): String = data
    }))

    env.execute()
  }
}
