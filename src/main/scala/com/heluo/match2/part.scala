package com.heluo.match2

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration

object part {
  def main(args: Array[String]): Unit = {
    val result2 = OutputTag[Int]("refundcountminute")
    val result3 = OutputTag[String]("cancelrate")

    val result1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "")
      .flatMap(new FlatMapFunction[String, Order] {
        override def flatMap(value: String, out: Collector[Order]): Unit = {
          try {
            val arr = value.split(",")
            out.collect(Order(arr.head,
              arr(3).toDouble,
              arr(4),
              new SimpleDateFormat("yyyy-MM-dd").parse(arr(10)).getTime,
              new SimpleDateFormat("yyyy-MM-dd").parse(arr(11)).getTime))
          } catch {
            case e: Exception => println(s"过滤不正常数据: ${value}")
          }
        }
      }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5L))
      .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = Math.max(element.createTime, element.operationTime)
      }))
      .process(new ProcessFunction[Order, Int] {

        var totalCount: Int = 0
        var refundCount: Int = 0
        var cancel: Int = 0
        var total: Int = 0

        override def processElement(value: Order, ctx: ProcessFunction[Order, Int]#Context, out: Collector[Int]): Unit = {
          total += 1
          val status = value.status
          if (status.equals("1001") || status.equals("1002") || status.equals("1004")) {
            totalCount += 1
            out.collect(totalCount)
          } else if (status.equals("1005")) {
            refundCount += 1
            ctx.output(result2, refundCount)
          } else if (status.equals("1003")) {
            cancel += 1
            ctx.output(result3, f"${cancel * 1D / total * 100}%.1f" + "%")
          }
        }
      }).setParallelism(1)

    result1.addSink(new RedisSink(redis, new RedisMapper[Int] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: Int): String = "totalcount"

      override def getValueFromData(data: Int): String = data.toString
    }))

    result1.getSideOutput(result2).addSink(new RedisSink(redis, new RedisMapper[Int] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: Int): String = "refundcountminute"

      override def getValueFromData(data: Int): String = data.toString
    }))

    result1.getSideOutput(result3).addSink(new RedisSink(redis, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: String): String = "cancelrate"

      override def getValueFromData(data: String): String = data
    }))

    env.execute()
  }
}
