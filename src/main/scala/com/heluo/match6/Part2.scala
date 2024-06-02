package com.heluo.match6

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable
import java.text.SimpleDateFormat
import java.time.Duration
import scala.language.postfixOps

object Part2 {
  def main(args: Array[String]): Unit = {
    val result2 = OutputTag[(String, BigDecimal)]("top3itemconsumption")

    val orderDetail = env.fromSource(getKafkaSource("orderDetail"), WatermarkStrategy.noWatermarks(), "orderName")
      .flatMap(new FlatMapFunction[String, OrderDetail] {
        override def flatMap(value: String, out: Collector[OrderDetail]): Unit = {
          try {
            val arr = value.split(",")
            out.collect(OrderDetail(
              arr.head,
              arr(2),
              arr(4).toDouble,
              arr(5).toInt,
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(6)).getTime
            ))
          } catch {
            case e: Exception => println(s"清洗不正常数据: ${value}")
          }
        }
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5L))
        .withTimestampAssigner(new SerializableTimestampAssigner[OrderDetail] {
          override def extractTimestamp(element: OrderDetail, recordTimestamp: Long): Long = element.createTime
        }))
      .keyBy(_.skuId)
      .process(new KeyedProcessFunction[String, OrderDetail, (String, Int)] {

        lazy val totalCountState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("totalCountState", classOf[Int]))
        lazy val totalAmountState: ValueState[BigDecimal] = getRuntimeContext.getState(new ValueStateDescriptor[BigDecimal]("totalAmountState", classOf[BigDecimal]))

        override def processElement(value: OrderDetail, ctx: KeyedProcessFunction[String, OrderDetail, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          val totalCount: Int = totalCountState.value()
          val totalAmount: BigDecimal = totalAmountState.value()
          var result: Int = 0
          var resultAmount: BigDecimal = BigDecimal(0.0).setScale(1)

          if (totalCount != 0) {
            result = totalCount + value.skuNum
            out.collect((ctx.getCurrentKey, result))
          } else {
            result = value.skuNum
          }

          if (totalAmount != null) {
            resultAmount = totalAmount + BigDecimal(value.skuNum * value.orderPrice).setScale(1)
            ctx.output(result2, (ctx.getCurrentKey, resultAmount))
          } else {
            resultAmount = BigDecimal(value.skuNum * value.orderPrice).setScale(1)
          }

          totalCountState.update(result)
          totalAmountState.update(resultAmount)
        }
      })

    orderDetail
      .keyBy(_ => true)
      .process(new KeyedProcessFunction[Boolean, (String, Int), String] {

        val map: mutable.Map[String, Int] = mutable.HashMap[String, Int]()

        override def processElement(value: (String, Int), ctx: KeyedProcessFunction[Boolean, (String, Int), String]#Context, out: Collector[String]): Unit = {
          map.put(value._1, value._2)
          ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 1L)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, (String, Int), String]#OnTimerContext, out: Collector[String]): Unit = {
          if (map.size >= 3){
            val tuples = map.toList
              .sortBy(_._2)
              .reverse
              .take(3)

            out.collect(s"[${tuples.head._1}:${tuples.head._2},${tuples(1)._1}:${tuples(1)._2},${tuples.last._1}:${tuples.last._2}]")
          }
        }
      })
      .addSink(new RedisSink[String](redisConf, new RedisMapper[String] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

        override def getKeyFromData(data: String): String = "top3itemamount"

        override def getValueFromData(data: String): String = data
      }))

    orderDetail.getSideOutput(result2)
      .keyBy(_ => true)
      .process(new KeyedProcessFunction[Boolean, (String, BigDecimal), String] {
        val map: mutable.Map[String, BigDecimal] = mutable.HashMap[String, BigDecimal]()

        override def processElement(value: (String, BigDecimal), ctx: KeyedProcessFunction[Boolean, (String, BigDecimal), String]#Context, out: Collector[String]): Unit = {
          map.put(value._1, value._2)
          ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 1L)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, (String, BigDecimal), String]#OnTimerContext, out: Collector[String]): Unit = {
          if (map.size >= 3){
            val tuples = map.toList
              .sortBy(_._2)
              .reverse
              .take(3)

            out.collect(s"[${tuples.head._1}:${tuples.head._2},${tuples(1)._1}:${tuples(1)._2},${tuples.last._1}:${tuples.last._2}]")
          }
        }
      })
      .addSink(new RedisSink[String](redisConf, new RedisMapper[String] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

        override def getKeyFromData(data: String): String = "top3itemconsumption"

        override def getValueFromData(data: String): String = data
      }))


    env.execute()
  }
}
