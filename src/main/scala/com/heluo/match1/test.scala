package com.heluo.match1

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.time.Duration

// 3447,计娴瑾,13208002474,6665,1004,5903,第4大街第25号楼6单元338门,描述396659,689816418657611,荣耀10青春版 幻彩渐变 2400万AI自拍 全网通版4GB+64GB 渐变蓝 移动联通电信4G全面屏手机 双卡双待等3件商品,2020-4-25 18:47:14,2020-4-25 18:47:14,29,5
// 订单状态分别为1001:创建订单、1002:支付订单、1003:取消订单、1004:完成订单、1005:申请退回、1006:退回完成。另外对于数据结果展示时，不要采用例如：1.9786518E7的科学计数法
object test {
  def main(args: Array[String]): Unit = {
    val result2 = OutputTag[BigDecimal]("totalrefundordercount")
    val result3 = OutputTag[Order]("shtd_result")

    val orderInfoStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "order_info")
      .flatMap(new FlatMapFunction[String, Order] {
        override def flatMap(value: String, out: Collector[Order]): Unit = {
          try {
            val arr = value.split(",")
            out.collect(Order(arr.head.toInt,
              arr(1),
              arr(2),
              arr(3).toDouble,
              arr(4),
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(10)).getTime,
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(11)).getTime,
              arr.last)
            )
          } catch {
            case e: Exception => println(s"过滤不正常数据: $value")
          }
        }
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5L))
        .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
          override def extractTimestamp(element: Order, recordTimestamp: Long): Long = Math.max(element.createTime, element.operationTime)
        }))
      .process(new ProcessFunction[Order, BigDecimal] {
        var totalAmount: Double = 0.0
        var refund: Double = 0.0

        override def processElement(value: Order, ctx: ProcessFunction[Order, BigDecimal]#Context, out: Collector[BigDecimal]): Unit = {
          val status = value.statue
          if (status.equals("1004") || status.equals("1002") || status.equals("1001")) {
            totalAmount = value.amount + totalAmount
            out.collect(BigDecimal(totalAmount).setScale(2))
          } else if (status.equals("1006")) {
            refund += value.amount
            ctx.output(result2, BigDecimal(refund).setScale(2))
          } else if (status.equals("1003")) {
            ctx.output(result3, value)
          }
        }
      }).setParallelism(1)

    orderInfoStream.addSink(new RedisSink[BigDecimal](redisConf, new RedisMapper[BigDecimal] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: BigDecimal): String = "totalprice"

      override def getValueFromData(data: BigDecimal): String = data.toString()
    })).setParallelism(1)

    orderInfoStream.getSideOutput(result2).addSink(new RedisSink[BigDecimal](redisConf, new RedisMapper[BigDecimal] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: BigDecimal): String = "totalrefundordercount"

      override def getValueFromData(data: BigDecimal): String = data.toString()
    })).setParallelism(1)

    orderInfoStream.getSideOutput(result3).addSink(JdbcSink.sink("insert into order_info values(?, ?, ?, ?, ?)",
      new JdbcStatementBuilder[Order] {
        override def accept(t: PreparedStatement, u: Order): Unit = {
          t.setString(1, u.id.toString)
          t.setString(2, u.consignee)
          t.setString(3, u.consignee_tel)
          t.setString(4, u.amount.toString)
          t.setString(5, u.feight_fee)
        }
      }, jdbcExecutionOptions,
      mysqlConf))

    env.execute()
  }
}
