package match7

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

import java.text.{DecimalFormat, SimpleDateFormat}
import java.time.Duration

import scala.collection.mutable

object path {
  def main(args: Array[String]): Unit = {
    // 8738,3556,14,Dior迪奥口红唇膏送女友老婆礼物生日礼物 烈艳蓝金999+888两支装礼盒,496,2,2020-4-26 18:55:01
    // 3444,慕容亨,13028730359,17805,1005,2015,第9大街第26号楼3单元383门,描述948496,226551358533723,Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待等2件商品,2020-4-25 18:47:14,2020-4-26 18:55:17,11,5

    val info = env.fromSource(getKafkaConf("OrderInfo"), WatermarkStrategy.noWatermarks(), "orderInfo")
      .map(e => {
        val arr = e.split(",")
        OrderInfo(arr.head, arr(3).toDouble, arr(4), arr(10), arr(11))
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[OrderInfo] {
          override def extractTimestamp(element: OrderInfo, recordTimestamp: Long): Long = {
            val create_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.create_time).getTime
            val operate_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.operate_time).getTime
            if (create_time > operate_time) create_time else operate_time
          }
        }))

    val detail = env.fromSource(getKafkaConf("OrderDetail"), WatermarkStrategy.noWatermarks(), "orderDetail")
      .map(e => {
        val arr = e.split(",")
        OrderDetail(arr(1), arr(2), arr(4).toDouble, arr(5).toInt, arr.last)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[OrderDetail] {
          override def extractTimestamp(element: OrderDetail, recordTimestamp: Long): Long = {
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.create_time).getTime
          }
        }))

    val result2 = OutputTag[String]("top3itemconsumption")

    val result = info.keyBy(_.id)
      .intervalJoin(detail.keyBy(_.orderID))
      .between(Time.seconds(-10), Time.seconds(10))
      .process(new ProcessJoinFunction[OrderInfo, OrderDetail, String] {
        var count = 0
        val map: mutable.Map[String, String] = mutable.HashMap[String, String]()

        override def processElement(left: OrderInfo, right: OrderDetail, ctx: ProcessJoinFunction[OrderInfo, OrderDetail, String]#Context, out: Collector[String]): Unit = {
          println(left.id + "<-------------->" + right.orderID)
          if (left.status == "1001" || left.status == "1002" || left.status == "1004") {
            count += 1
            out.collect(count.toString)
          }

          map.put(right.sku_id, new DecimalFormat("0.00").format(map.getOrElse(right.sku_id, "0").toDouble + right.sku_num * right.price))
          if (map.size >= 3) {
            val tuples: List[(String, String)] = map.toList.sortBy(_._2.toDouble).reverse.take(3)
            ctx.output(result2, "[" + tuples.head._1 + ":" + tuples.head._2 + "," + tuples(1)._1 + ":" + tuples(1)._2 + "," + tuples.last._1 + ":" + tuples.last._2 + "]")
          }
        }
      })

    val result3 = info.filter(e => e.status == "1001" || e.status == "1002" || e.status == "1004")
      .join(detail)
      .where(_.id)
      .equalTo(_.orderID)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .apply(new JoinFunction[OrderInfo, OrderDetail, (Int, String, String)] {
        override def join(first: OrderInfo, second: OrderDetail): (Int, String, String) = (first.id.toInt, first.final_total_amount.toString, second.sku_num.toString)
      })

    result.addSink(new RedisSink[String](redisConf, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: String): String = "totalcount"

      override def getValueFromData(data: String): String = data
    }))

    result.getSideOutput(result2).addSink(new RedisSink[String](redisConf, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: String): String = "top3itemconsumption"

      override def getValueFromData(data: String): String = data
    }))

    result3.addSink(new RichSinkFunction[(Int, String, String)] {
      var conn: Connection = _
      var mutator: BufferedMutator = _
      val namespace: String = "shtd_result"
      val table: String = "orderpositiveaggr"
      val columnFamily: String = "info"

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
//        val configuration = HBaseConfiguration.create()
//        configuration.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1,bigdata2,bigdata3")
        conn = ConnectionFactory.createConnection()
      }

      override def invoke(value: (Int, String, String), context: SinkFunction.Context): Unit = {
        super.invoke(value, context)
        mutator = conn.getBufferedMutator(TableName.valueOf(namespace, table))
        val put = new Put(Bytes.toBytes(value._1.toString))

        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes(value._1.toString))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("orderprice"), Bytes.toBytes(value._2))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("orderdetailcount"), Bytes.toBytes(value._3))

        mutator.mutate(put)

        if (mutator != null) mutator.close()
      }

      override def close(): Unit = {
        super.close()
        conn.close()
      }
    })


    env.execute()
  }
}
