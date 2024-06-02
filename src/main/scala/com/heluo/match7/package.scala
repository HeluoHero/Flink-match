package com.heluo

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

package object match7 {
  case class OrderInfo(id: String, orderPrice: Double, status: String, createTime: Long, operationTime: Long)

  case class OrderDetail(orderId: String, skuId: String, price: Double, orderDetailCount: Int, createTime: Long)


  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)

  def getKafkaSource(topicName: String): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setGroupId("")
      .setTopics(topicName)
      .setBootstrapServers("bigdata1:9092,bigdata2:9092,bigdata3:9092")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
  }

  val redisConf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
    .setHost("bigdata1")
    .setPort(6379)
    .build()

}
