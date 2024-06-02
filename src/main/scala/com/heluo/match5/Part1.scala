package com.heluo.match5


import com.heluo.match5.comment.BaseApp
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

object Part1 extends BaseApp {
  def main(args: Array[String]): Unit = {
    Part1.start(10011, 4, "flume-kafka", "ProduceRecord")
  }

  override def handle(stream: DataStream[String]): Unit = {
    stream.flatMap(new FlatMapFunction[String, ProduceRecord] {
      override def flatMap(value: String, out: Collector[ProduceRecord]): Unit = {
        try {
          val arr = value.split(",")
          out.collect(ProduceRecord(arr(1), arr.last))
        } catch {
          case _: Exception => println(s"过滤不正常数据: ${value}")
        }
      }
    }).filter(_.check == "1")
      .keyBy(_.id)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(5L)))
      .process(new ProcessWindowFunction[ProduceRecord, (String, String, String), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[ProduceRecord], out: Collector[(String, String, String)]): Unit = {
          out.collect((s"$key-${DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss.SSS")}", key, elements.size.toString))
        }
      }).addSink(new WebSocketSink)
      /*.addSink(new RichSinkFunction[(String, String, String)] {
        var conn: Connection = _
        val nameSpace = "gyflinkresult"
        val tableName = "Produce5minAgg"
        val columnFamily = "info"


        override def open(parameters: Configuration): Unit = {
          val conf = HBaseConfiguration.create()
          conf.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1,bigdata2,bigdata3")
          conn = ConnectionFactory.createConnection(conf)
        }

        override def invoke(value: (String, String, String), context: SinkFunction.Context): Unit = {
          val table = conn.getTable(TableName.valueOf(Bytes.toBytes(nameSpace), Bytes.toBytes(tableName)))
          val put = new Put(Bytes.toBytes(value._1))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("machine_id"), Bytes.toBytes(value._2))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("total_produce"), Bytes.toBytes(value._3))

          table.put(put)

          if (table != null) table.close()
        }

        override def close(): Unit = {
          conn.close()
        }
      })*/


  }
}
