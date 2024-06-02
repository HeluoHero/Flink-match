package com.heluo.match4

import com.heluo.match4.comment.BaseApp
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

object Part1 extends BaseApp {
  def main(args: Array[String]): Unit = {
    Part1.start(10010, 4, "part1", "ProduceRecord")
  }

  override def handle(env: StreamExecutionEnvironment, stream: DataStream[String]): Unit = {
    val result = stream.flatMap(new FlatMapFunction[String, ProduceRecord] {
      override def flatMap(value: String, out: Collector[ProduceRecord]): Unit = {
        val arr = value.split(",")
        out.collect(ProduceRecord(arr(1), arr.last))
      }
    }).filter(_.status == "1")
      .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5L)))
      .process(new ProcessAllWindowFunction[ProduceRecord, (String, Int), TimeWindow] {
        override def process(context: Context, elements: Iterable[ProduceRecord], out: Collector[(String, Int)]): Unit = {
          elements.groupBy(_.id)
            .mapValues(_.size)
            .foreach(out.collect)
        }
      })
    result
      .addSink(new RedisSink[(String, Int)](redisConf, new RedisMapper[(String, Int)] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "totalproduce")

        override def getKeyFromData(data: (String, Int)): String = data._1

        override def getValueFromData(data: (String, Int)): String = data._2.toString
      }))

  }
}
