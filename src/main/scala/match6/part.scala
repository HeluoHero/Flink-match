package match6

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable
import java.text.{DecimalFormat, SimpleDateFormat}
import java.time.Duration

object part {
  def main(args: Array[String]): Unit = {
    //3861,沈晓明,13433851589,15545.66,1006,4887,第10大街第40号楼9单元311门,描述723751,822561443917375,勒姆森无线蓝牙耳机头戴式重低音电脑通用,2020-4-26 23:54:47,2020-4-26 23:54:53,11,17
    //8624,3446,5,十月稻田 沁州黄小米 (黄小米 五谷杂粮 山西特产 真空装 大米伴侣 粥米搭档) 2.5kg,244,1,2020-4-25 18:47:14
    val order_info = env.fromSource(getKafkaConf("OrderInfo"), WatermarkStrategy.noWatermarks(), "order_info")
      .map(e => {
        val arr = e.split(",")
        //        println(info(arr(0), arr(4), arr(10), arr(11)))
        info(arr(0), arr(4), arr(10), arr(11))
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[info] {
          override def extractTimestamp(element: info, recordTimestamp: Long): Long = {
            val createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.create_time).getTime
            val operateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.operation_time).getTime
            if (operateTime > createTime) operateTime else createTime
          }
        }))

    val order_detail = env.fromSource(getKafkaConf("OrderDetail"), WatermarkStrategy.noWatermarks(), "order_detail")
      .map(e => {
        val arr = e.split(",")
        detail(arr(1), arr(2), arr(4).toDouble, arr(5).toInt, arr.last)
        //        println(detail(arr(1), arr(2), arr(4).toDouble, arr(5).toInt, arr.last))
        detail(arr(1), arr(2), arr(4).toDouble, arr(5).toInt, arr.last)
      }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[detail] {
        override def extractTimestamp(element: detail, recordTimestamp: Long): Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.create_time).getTime
      }))

    val result2 = OutputTag[String]("top3itemamount")
    val result3 = OutputTag[String]("top3itemconsumption")
    //    val result4 = OutputTag[String]("top3itemconsumption")

    val result = order_info.keyBy(_.id)
      .intervalJoin(order_detail.keyBy(_.order_id))
      .between(Time.seconds(-30), Time.seconds(30))
      .process(new ProcessJoinFunction[info, detail, Int] {
        var count = 0
        val map1: mutable.Map[String, Int] = mutable.HashMap[String, Int]()
        val map2: mutable.Map[String, String] = mutable.HashMap[String, String]()

        override def processElement(left: info, right: detail, ctx: ProcessJoinFunction[info, detail, Int]#Context, out: Collector[Int]): Unit = {
          print(left.id + "<-------------->" + right.order_id)
          if (left.status == "1001" || left.status == "1002" || left.status == "1004") {
            count += 1
            out.collect(count)
          }

          map1.put(right.sku_id, map1.getOrElse(right.sku_id, 0) + right.sku_num)
          if (map1.size >= 3) {
            val tuples: List[(String, Int)] = map1.toList.sortBy(_._2).reverse.take(3)
            val str = "[" + tuples.head._1 + ":" + tuples.head._2 + "," + tuples(1)._1 + ":" + tuples(1)._2 + "," + tuples.last._1 + ":" + tuples.last._2 + "]"
            ctx.output(result2, str)
          }

          map2.put(right.sku_id, new DecimalFormat("0.00").format(map2.getOrElse(right.sku_id, "0").toDouble + right.sku_num * right.order_price))
          if (map2.size >= 3) {
            val tuples = map2.toList.sortBy(_._2.toDouble).reverse.take(3)
            val str = "[" + tuples.head._1 + ":" + tuples.head._2 + "," + tuples(1)._1 + ":" + tuples(1)._2 + "," + tuples.last._1 + ":" + tuples.last._2 + "]"
            ctx.output(result3, str)
          }
        }
      })

    result.print()
    result.getSideOutput(result2).printToErr()
    result.getSideOutput(result3).print()

    /*result.addSink(new RedisSink[Int](redis, new RedisMapper[Int]() {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: Int): String = "totalcount"

      override def getValueFromData(data: Int): String = data.toString
    }))

    result.getSideOutput(result2).addSink(new RedisSink[String](redis, new RedisMapper[String]() {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: String): String = "top3itemamount"

      override def getValueFromData(data: String): String = data
    }))

    result.getSideOutput(result3).addSink(new RedisSink[String](redis, new RedisMapper[String]() {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: String): String = "top3itemconsumption"

      override def getValueFromData(data: String): String = data
    }))
*/

    env.execute()
  }
}
