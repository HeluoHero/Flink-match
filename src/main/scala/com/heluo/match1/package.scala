package com.heluo

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

package object match1 {
  case class Order(id: Int, consignee: String, consignee_tel: String, amount: Double, statue: String, createTime: Long, operationTime: Long, feight_fee: String)

  val conf: Configuration = new Configuration()
  conf.setInteger("rest.port", 10011)

  //  conf.setInteger(RestOptions.PORT, 10011)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env.setParallelism(4)

  val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
    .setGroupId("order_info")
    .setTopics("order")
    .setBootstrapServers("bigdata1:9092,bigdata2:9092,bigdata3:9092")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema)
    .build()

  val redisConf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
    .setHost("bigdata1")
    .setPort(6379)
    .build()

  val mysqlConf: JdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    .withUrl("jdbc:mysql://bigdata1:3306/shtd_result?useSSL=false&characterEncoding=UTF-8")
    .withUsername("root")
    .withPassword("123456")
    .withConnectionCheckTimeoutSeconds(60)
    .build()

  val jdbcExecutionOptions: JdbcExecutionOptions = JdbcExecutionOptions.builder()
    .withMaxRetries(3)
    .withBatchSize(100)
    .withBatchIntervalMs(3000)
    .build()

}
