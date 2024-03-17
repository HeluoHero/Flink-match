package match8

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.mutable

object part2 {
  def main(args: Array[String]): Unit = {

    // 20_268_662,113,20,预警,2024-03-12 10:33:57,2024-03-12 10:35:17,20
    val result = env.fromSource(getKafkaConf("ChangeRecord"), WatermarkStrategy.noWatermarks(), "part2")
      .map(e => {
        val arr = e.split(",")
        Change(arr(1), arr(3))
      })
      .filter(_.status == "预警")
      .keyBy(_.id)
      .window(SlidingProcessingTimeWindows.of(Time.minutes(3), Time.minutes(1)))
      .process(new ProcessWindowFunction[Change, (String, Int, Long), String, TimeWindow] {

        override def process(key: String, context: Context, elements: Iterable[Change], out: Collector[(String, Int, Long)]): Unit = {
          out.collect((key, elements.size, context.window.getEnd))
        }
      })
      .keyBy(_._3)
      .process(new KeyedProcessFunction[Long, (String, Int, Long), (String, String)] {

        lazy val mapState: MapState[String, Int] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Int]("vcCountMapState", classOf[String], classOf[Int]))

        override def processElement(value: (String, Int, Long), ctx: KeyedProcessFunction[Long, (String, Int, Long), (String, String)]#Context, out: Collector[(String, String)]): Unit = {
          if (mapState.isEmpty) {
            ctx.timerService().registerProcessingTimeTimer(value._3 + 500L)
          }
          mapState.put(value._1, value._2)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, (String, Int, Long), (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
          super.onTimer(timestamp, ctx, out)
          val map = mutable.HashMap[String, Int]()
          mapState.entries().forEach(e => map.put(e.getKey, e.getValue))
          val tuple = map.toList.maxBy(_._2)
          out.collect((DateFormatUtils.format(ctx.getCurrentKey, "yyyy-MM-dd HH:mm:ss"), tuple._1))
          mapState.clear()
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
