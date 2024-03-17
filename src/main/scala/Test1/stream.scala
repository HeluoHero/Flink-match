package Test1

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

object stream {
  def main(args: Array[String]): Unit = {
    env.setParallelism(1)

    val stream = env.fromSource(KafkaSource.builder[String]
      .setTopics("")
      .setBootstrapServers("bigdata1:9092")
      .setGroupId("consumer-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build(), WatermarkStrategy.noWatermarks(), "")

    stream.map(e => e.split(",")(0))
      .addSink(new RichSinkFunction[String] {
        var conn: Connection = _
        var mutator: BufferedMutator = _
        val namespace = ""
        val tableName = ""
        val rowKey = ""
        val columnFamily = ""
        val columnName = ""

        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          val configuration = HBaseConfiguration.create()
          configuration.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1,bigdata2,bigdata3")
          conn = ConnectionFactory.createConnection(configuration)
        }

        override def invoke(value: String, context: SinkFunction.Context): Unit = {
          val params = new BufferedMutatorParams(TableName.valueOf(namespace, tableName))
          params.writeBufferSize(5 * 1024 * 1024) // 当文件数据量大于5MB，则停止写入
          params.setWriteBufferPeriodicFlushTimeoutMs(3000L) // 当写入时间超过三秒，停止写入
          mutator = conn.getBufferedMutator(params)

          val put = new Put(Bytes.toBytes(rowKey))

          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value))

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
