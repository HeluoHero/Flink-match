package match3

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

import java.text.SimpleDateFormat
import java.time.Duration
import scala.collection.mutable

case class order(id: Int, consignee: String, consignee_tel: String, final_total_amount: Double, order_status: String,
                 user_id: Int, delivery_address: String, order_comment: String, out_trade_no: String, trade_body: String,
                 create_time: String, operate_time: String, province_id: Int)

object Part {
  def main(args: Array[String]): Unit = {
    val result2 = OutputTag[String]("cancelrate")
    val result3 = OutputTag[order]("hbase")

    val result1 = env.fromSource(kafkaConf, WatermarkStrategy.noWatermarks(), "")
      .map(e => {
        val arr = e.split(",")
        order(arr.head.toInt, arr(1), arr(2), arr(3).toDouble, arr(4), arr(5).toInt, arr(6), arr(7), arr(8), arr(9), arr(10), arr(11), arr.last.toInt)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[order] {
          override def extractTimestamp(element: order, recordTimestamp: Long): Long = {
            val createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.create_time).getTime
            val operateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.operate_time).getTime
            if (operateTime > createTime) operateTime else createTime
          }
        }))
      .keyBy(_.user_id)
      .process(new KeyedProcessFunction[Int, order, String] {
        lazy val lastVcState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("total_money", classOf[Double]))
        val map: mutable.HashMap[String, Double] = mutable.HashMap()
        var total = 0
        var cancel = 0

        override def processElement(value: order, ctx: KeyedProcessFunction[Int, order, String]#Context, out: Collector[String]): Unit = {
          total += 1
          val status = value.order_status
          if (status == "1001" || status == "1002" || status == "1004") {
            val total_money = value.final_total_amount + lastVcState.value()
            map.put(ctx.getCurrentKey + ":" + value.consignee, total_money)

            val arr = map.toList.sortBy(_._2).reverse

            if (arr.size < 2) {
              out.collect("[" + arr.head._1 + ":" + arr.head._2 + "]")
            } else {
              val first = arr.head
              val second = arr(1)

              out.collect("[" + first._1 + ":" + first._2 + "," + second._1 + ":" + second._2 + "]")
            }
            lastVcState.update(total_money)
          } else if (status == "1003"){
            cancel += 1
            val cancelrate = f"${cancel * 1D / total * 100}%.1f" + "%"
            ctx.output(result2, cancelrate)
            ctx.output(result3, value)
          }
        }
      })

    result1.print()
    result1.getSideOutput(result2).printToErr()
    result1.getSideOutput(result3).print()

    result1.addSink(new RedisSink[String](redisConf, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET, "")

      override def getKeyFromData(data: String): String = "top2userconsumption"

      override def getValueFromData(data: String): String = data
    }))

    result1.getSideOutput(result2).addSink(new RedisSink[String](redisConf, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET, "")

      override def getKeyFromData(data: String): String = "cancelrate"

      override def getValueFromData(data: String): String = data
    }))

    result1.getSideOutput(result3).addSink(new RichSinkFunction[order] {
      var conn: Connection = _
      var mutator: BufferedMutator = _
      val namespace = "shtd_result"
      val tableName = "order_info"
      val columnFamily = "info"

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val configuration = HBaseConfiguration.create()
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1,bigdata2,bigdata3")
        conn = ConnectionFactory.createConnection(configuration)
      }

      override def invoke(value: order, context: SinkFunction.Context): Unit = {
        super.invoke(value, context)
        val params = new BufferedMutatorParams(TableName.valueOf(namespace, tableName))
        params.writeBufferSize(5 * 1024 * 1024) // 当文件数据量大于5MB，则停止写入
        params.setWriteBufferPeriodicFlushTimeoutMs(3000L) // 当写入时间超过三秒，停止写入
        mutator = conn.getBufferedMutator(params)

        val put = new Put(Bytes.toBytes(value.id.toString))

        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes(value.id))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("consignee"), Bytes.toBytes(value.consignee))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("consignee_tel"), Bytes.toBytes(value.consignee_tel))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("final_total_amount"), Bytes.toBytes(value.final_total_amount))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("order_status"), Bytes.toBytes(value.order_status))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("user_id"), Bytes.toBytes(value.user_id))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("delivery_address"), Bytes.toBytes(value.delivery_address))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("order_comment"), Bytes.toBytes(value.order_comment))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("out_trade_no"), Bytes.toBytes(value.out_trade_no))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trade_body"), Bytes.toBytes(value.trade_body))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("create_time"), Bytes.toBytes(value.create_time))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("operate_time"), Bytes.toBytes(value.operate_time))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("province_id"), Bytes.toBytes(value.province_id))

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
