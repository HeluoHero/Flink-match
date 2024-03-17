package match5

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

object part1 {
  def main(args: Array[String]): Unit = {
    val result = env.fromSource(getKafkaConf("ProduceRecord"), WatermarkStrategy.noWatermarks(), "")
      .map(e => {
        val arr = e.split(",")
        produce(arr(1), arr.last)
      })
      .filter(_.changeHandleState == "1")
      .keyBy(_.id)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
      .process(new ProcessWindowFunction[produce, (String, String, String), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[produce], out: Collector[(String, String, String)]): Unit = {
          out.collect((key + "-" + DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"), key, elements.size.toString))
        }
      })

    result.print()
    result.addSink(new RichSinkFunction[(String, String, String)] {
      var conn: Connection = _
      var params: BufferedMutatorParams = _
      var mutator: BufferedMutator = _
      val nameSpace = "gyflinkresult"
      val table = "Produce5minAgg"
      val columnFamily = "info"

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val configuration = HBaseConfiguration.create()
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1,bigdata2,bigdata3")
        conn = ConnectionFactory.createConnection(configuration)

        params = new BufferedMutatorParams(TableName.valueOf(nameSpace, table))
        params.setWriteBufferPeriodicFlushTimeoutMs(3000L)
        params.writeBufferSize(5 * 1024 * 1024)
      }


      override def invoke(value: (String, String, String), context: SinkFunction.Context): Unit = {
        super.invoke(value, context)
        mutator = conn.getBufferedMutator(params)
        val put = new Put(Bytes.toBytes(value._1))

        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("machine_id"), Bytes.toBytes(value._2))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("total_produce"), Bytes.toBytes(value._3))

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
