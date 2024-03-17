package match1

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import java.sql.PreparedStatement
import java.text.DecimalFormat

case class Order(id:String, consignee:String, consignee_tel: String, final_total_amount: Double, order_status: Int, feight_fee: String)
// "3590", "黄启", "13437150533", "9981", "1001", "7350", "第19大街第11号楼8单元389门", "描述779912", "869225353414456", "TCL 55A950C 55英寸32核人工智能 HDR曲面超薄4K电视金属机身（枪色）等3件商品", "2020-4-26 18:55:01", "2020-4-26 19:03:49", "19", "18"
object Part {
  def main(args: Array[String]): Unit = {
    val result2 = OutputTag[Double]("totalrefundordercount")
    val result3 = OutputTag[Order]("shtd_result")

    val result1 = env.fromSource(kafkaSet, WatermarkStrategy.noWatermarks(), "")
      .map(e => {
        val str: String = e.replaceAll("\"", "")
        val arr = str.split(",")
        Order(arr.head, arr(1), arr(2), arr(3).toDouble, arr(4).toInt, arr.last)
      })
      .keyBy(e => true)
      .process(new KeyedProcessFunction[Boolean, Order, Double] {
        var totalprice = 0.0
        var totalrefundordercount = 0.0

        override def processElement(value: Order, ctx: KeyedProcessFunction[Boolean, Order, Double]#Context, out: Collector[Double]): Unit = {
          if (value.order_status == 1002 || value.order_status == 1004) {
            totalprice += value.final_total_amount
//            val money = new DecimalFormat("0.00").format(result)
            out.collect(totalprice)
          } else if (value.order_status == 1006) {
            totalrefundordercount += value.final_total_amount
            ctx.output(result2, totalrefundordercount)
          } else if (value.order_status == 1003) {
            ctx.output(result3, value)
          }
        }
      })

    result1.addSink(new RedisSink[Double](redisConf, new RedisMapper[Double] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET, "totalprice")

      override def getKeyFromData(data: Double): String = "totalprice"

      override def getValueFromData(data: Double): String = data.toString
    }))

    result1.getSideOutput(result2).addSink(new RedisSink[Double](redisConf, new RedisMapper[Double] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET, "totalrefundordercount")

      override def getKeyFromData(data: Double): String = "totalrefundordercount"

      override def getValueFromData(data: Double): String = data.toString
    }))

    result1.getSideOutput(result3).addSink(JdbcSink.sink("insert into order_info values(?, ?, ?, ?, ?)",
      new JdbcStatementBuilder[Order] {
        override def accept(t: PreparedStatement, u: Order): Unit = {
          t.setString(1, u.id)
          t.setString(2, u.consignee)
          t.setString(3, u.consignee_tel)
          t.setString(4, u.final_total_amount.toString)
          t.setString(5, u.feight_fee)
        }
      }, jdbcExecutionOptions,
      mysqlConf))

    env.execute()
  }
}
