package com.heluo.match7

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{FlatMapFunction, JoinFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import java.text.SimpleDateFormat

object Part {
  def main(args: Array[String]): Unit = {
    val orderInfo = env.fromSource(getKafkaSource("order"), WatermarkStrategy.noWatermarks(), "orderInfo")
      .flatMap(new FlatMapFunction[String, OrderInfo] {
        override def flatMap(value: String, out: Collector[OrderInfo]): Unit = {
          try {
            val arr = value.split(",")
            out.collect(OrderInfo(arr.head,
              arr(3).toDouble,
              arr(4),
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(10)).getTime,
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(11)).getTime))
          } catch {
            case e: Exception => {
              println(s"清洗不正常数据: $value")
            }
          }
        }
      }).filter(e => e.status.equals("1001") || e.status.equals("1002") || e.status.equals("1004"))

    val orderDetail = env.fromSource(getKafkaSource("orderDetail"), WatermarkStrategy.noWatermarks(), "orderDetail")
      .flatMap(new FlatMapFunction[String, OrderDetail] {
        override def flatMap(value: String, out: Collector[OrderDetail]): Unit = {
          try {
            val arr = value.split(",")
            out.collect(OrderDetail(
              arr(1),
              arr(2),
              arr(4).toDouble,
              arr(5).toInt,
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(6)).getTime
            ))
          } catch {
            case e: Exception => {
              println(s"清洗不正常数据: $value")
            }
          }
        }
      })

    orderInfo.process(new ProcessFunction[OrderInfo, Int] {
      var count: Int = 0

      override def processElement(value: OrderInfo, ctx: ProcessFunction[OrderInfo, Int]#Context, out: Collector[Int]): Unit = {
        count += 1
        out.collect(count)
      }
    }).setParallelism(1)
      .addSink(new RedisSink[Int](redisConf, new RedisMapper[Int]() {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

        override def getKeyFromData(data: Int): String = "totalcount"

        override def getValueFromData(data: Int): String = data.toString
      }))

    orderDetail.keyBy(_.skuId)
      .process(new KeyedProcessFunction[String, OrderDetail, (String, BigDecimal)] {

        lazy val consumerState: ValueState[BigDecimal] = getRuntimeContext.getState(new ValueStateDescriptor[BigDecimal]("consumerState", classOf[BigDecimal]))

        override def processElement(value: OrderDetail, ctx: KeyedProcessFunction[String, OrderDetail, (String, BigDecimal)]#Context, out: Collector[(String, BigDecimal)]): Unit = {
          val consume = consumerState.value()
          var result: BigDecimal = BigDecimal(0.0).setScale(1)
          if (consume != null) {
            result = consume + BigDecimal(value.price * value.orderDetailCount).setScale(1)
            out.collect(ctx.getCurrentKey, result)
          } else {
            result = BigDecimal(value.price * value.orderDetailCount).setScale(1)
          }
          consumerState.update(result)
        }
      })
      .keyBy(_ => true)
      .process(new KeyedProcessFunction[Boolean, (String, BigDecimal), String] {

        val map: mutable.Map[String, BigDecimal] = mutable.HashMap[String, BigDecimal]()

        override def processElement(value: (String, BigDecimal), ctx: KeyedProcessFunction[Boolean, (String, BigDecimal), String]#Context, out: Collector[String]): Unit = {
          map.put(value._1, value._2)
          ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 1L)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, (String, BigDecimal), String]#OnTimerContext, out: Collector[String]): Unit = {
          if (map.size > 2) {
            val tuples = map.toList
              .sortBy(_._2)
              .reverse
              .take(3)

            out.collect(s"[${tuples.head._1}:${tuples.head._2},${tuples(1)._1}:${tuples(1)._2},${tuples.last._1}:${tuples.last._2}]")
          }
        }
      })
      .setParallelism(1)
      .addSink(new RedisSink[String](redisConf, new RedisMapper[String]() {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

        override def getKeyFromData(data: String): String = "top3itemconsumption"

        override def getValueFromData(data: String): String = data
      }))

    orderInfo.join(orderDetail)
      .where(_.id)
      .equalTo(_.orderId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
      .apply(new JoinFunction[OrderInfo, OrderDetail, (Int, String, String)] {
        override def join(first: OrderInfo, second: OrderDetail): (Int, String, String) = {
          (first.id.toInt, first.orderPrice.toString, second.orderDetailCount.toString)
        }
      })
      .addSink(new RichSinkFunction[(Int, String, String)] {

        var conn: Connection = _
        val namespace = "shtd_result"
        val tableName = "orderpositiveaggr"
        val columnFamily = "info"

        override def open(parameters: Configuration): Unit = {
          val conf = HBaseConfiguration.create()
          conf.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1,bigdata2,bigdata3")
          conn = ConnectionFactory.createConnection(conf)
        }

        override def invoke(value: (Int, String, String), context: SinkFunction.Context): Unit = {
          println(value)
          val table = conn.getTable(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tableName)))

          val put = new Put(Bytes.toBytes(value._1.toString))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes(value._1.toString))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("orderprice"), Bytes.toBytes(value._2))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("orderdetailcount"), Bytes.toBytes(value._3))

          table.put(put)

          if (table != null) table.close()
        }

        override def close(): Unit = {
          conn.close()
        }
      })


    env.execute()
  }
}
