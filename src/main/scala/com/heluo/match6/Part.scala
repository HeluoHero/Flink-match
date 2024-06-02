package com.heluo.match6

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration

object Part {
  def main(args: Array[String]): Unit = {
    env.fromSource(getKafkaSource("order"), WatermarkStrategy.noWatermarks(), "")
      .flatMap(new FlatMapFunction[String, OrderInfo] {
        override def flatMap(value: String, out: Collector[OrderInfo]): Unit = {
          try {
            val arr = value.split(",")

            out.collect(OrderInfo(arr.head,
              arr(4),
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(10)).getTime,
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(11)).getTime))
          } catch {
            case _: Exception => println(s"过滤不正常数据: ${value}")
          }
        }
      }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5L))
      .withTimestampAssigner(new SerializableTimestampAssigner[OrderInfo] {
        override def extractTimestamp(element: OrderInfo, recordTimestamp: Long): Long = Math.max(element.createTime, element.operationTime)
      }))
      .filter(e => e.status.equals("1001") || e.status.equals("1002") || e.status.equals("1004"))
      .process(new ProcessFunction[OrderInfo, Int] {

        var count: Int = 0

        override def processElement(value: OrderInfo, ctx: ProcessFunction[OrderInfo, Int]#Context, out: Collector[Int]): Unit = {
          count += 1
          out.collect(count)
        }
      }).setParallelism(1)
      .addSink(new RedisSink[Int](redisConf, new RedisMapper[Int] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

        override def getKeyFromData(data: Int): String = "totalcount"

        override def getValueFromData(data: Int): String = data.toString
      })).setParallelism(1)

    env.execute()
  }
}
