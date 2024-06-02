package com.heluo.match5

import com.heluo.match5.comment.BaseApp
import com.heluo.match5.function.ChangeRecordFlatMapFunction
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

object Part3 extends BaseApp {
  def main(args: Array[String]): Unit = {
    Part3.start(10013, 4, "cccc", "ChangeRecord")
  }

  override def handle(stream: DataStream[String]): Unit = {
    stream.flatMap(new ChangeRecordFlatMapFunction)
      .filter(_.status == "预警")
      .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(3), Time.minutes(1)))
      .process(new ProcessAllWindowFunction[ChangeRecord, (String, String), TimeWindow] {
        override def process(context: Context, elements: Iterable[ChangeRecord], out: Collector[(String, String)]): Unit = {
          out.collect((DateFormatUtils.format(context.window.getEnd, "yyyy-MM-dd HH:mm:ss"),
            elements.groupBy(_.id)
            .mapValues(_.size)
            .maxBy(_._2)._1))
        }
      })
      .addSink(new RedisSink[(String, String)](redisConf, new RedisMapper[(String, String)] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "warning_last3min_everymin_out")

        override def getKeyFromData(data: (String, String)): String = data._1

        override def getValueFromData(data: (String, String)): String = data._2
      }))

  }
}
