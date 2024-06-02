package com.heluo

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

package object match2 {
  case class Order(id: String, amount: Double, status: String, createTime: Long, operationTime: Long)

  val conf = new Configuration()
  conf.setInteger("rest.port", 10011)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env.setParallelism(4)

  val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
    .setGroupId("part2")
    .setTopics("order")
    .setBootstrapServers("bigdata1:9092,bigdata2:9092,bigdta3:9092")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build()

  val redis: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
    .setHost("bigdata1")
    .setPort(6379)
    .build()
}
