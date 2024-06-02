package com.heluo.match3

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

import scala.collection.mutable
import java.text.SimpleDateFormat
import java.time.Duration

object test {
  def main(args: Array[String]): Unit = {

    val result2 = OutputTag[String]("cancelrate")
    val result3 = OutputTag[Order]("hbasedata")

    val result1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "test3")
      .flatMap(new FlatMapFunction[String, Order] {
        override def flatMap(value: String, out: Collector[Order]): Unit = {
          try {
            val arr = value.split(",")
            out.collect(Order(arr.head.toInt,
              arr(5),
              arr(1),
              arr(3).toDouble,
              arr(4),
              new SimpleDateFormat("yyyy-MM-dd").parse(arr(10)).getTime,
              new SimpleDateFormat("yyyy-MM-dd").parse(arr(11)).getTime))
          } catch {
            case e: Exception => println(s"过滤不正常数据：${value}")
          }
        }
      }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5L))
      .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = Math.max(element.createTime, element.operationTime)
      }))
      .process(new ProcessFunction[Order, String] {

        var cancel: Int = 0
        var total: Int = 0
        val userMap: mutable.Map[String, BigDecimal] = mutable.HashMap[String, BigDecimal]()

        override def processElement(value: Order, ctx: ProcessFunction[Order, String]#Context, out: Collector[String]): Unit = {
          total += 1
          val status = value.status
          if (status.equals("1001") || status.equals("1002") || status.equals("1004")) {
            val key = value.userId + ":" + value.consignee
            userMap.put(key, (userMap.getOrElse(key, BigDecimal(0)) + BigDecimal(value.amount)).setScale(2, BigDecimal.RoundingMode.HALF_UP))
            if (userMap.size > 1) {
              val tuple: List[(String, BigDecimal)] = userMap.toList.sortBy(_._2).reverse.take(2)
              out.collect(s"[${tuple.head._1}:${tuple.head._2},${tuple.last._1}:${tuple.last._2}]")
            }
          } else if (status.equals("1003")) {
            cancel += 1
            ctx.output(result2, s"${BigDecimal(cancel * 1D / total * 100).setScale(1, BigDecimal.RoundingMode.HALF_UP)}%")
            ctx.output(result3, value)
          }
        }
      }).setParallelism(1)

//    result1.print()
//    result1.getSideOutput(result2).printToErr()

    result1.addSink(new RedisSink[String](redisConf, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: String): String = "top2userconsumption"

      override def getValueFromData(data: String): String = data
    }))

    result1.getSideOutput(result2).addSink(new RedisSink[String](redisConf, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: String): String = "cancelrate"

      override def getValueFromData(data: String): String = data
    }))
    result1.getSideOutput(result3).addSink(new RichSinkFunction[Order] {
      val namespace = "shtd_result"
      val tableName = "order_info"
      val familyColumn = "info"
      var conn: Connection = _

      override def open(parameters: configuration.Configuration): Unit = {
        val conf: Configuration = HBaseConfiguration.create()
        conf.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1,bigdata2,bigdata3")
        conn = ConnectionFactory.createConnection(conf)
      }

      override def invoke(value: Order, context: SinkFunction.Context): Unit = {
        super.invoke(value, context)
        val table = conn.getTable(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tableName)))

        val put = new Put(Bytes.toBytes(value.id.toString))
        put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes("id"), Bytes.toBytes(value.id.toString))
        put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes("consignee"), Bytes.toBytes(value.consignee))
        put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes("final_total_amount"), Bytes.toBytes(value.amount.toString))
        put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes("order_status"), Bytes.toBytes(value.status))
        put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes("user_id"), Bytes.toBytes(value.userId))
        put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes("create_time"), Bytes.toBytes(DateFormatUtils.format(value.createTime, "yyyy-MM-dd HH:mm:ss")))
        put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes("operation_time"), Bytes.toBytes(DateFormatUtils.format(value.operationTime, "yyyy-MM-dd HH:mm:ss")))

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
