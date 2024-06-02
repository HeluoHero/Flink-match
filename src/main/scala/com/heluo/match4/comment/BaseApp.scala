package com.heluo.match4.comment

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

trait BaseApp {
  def start(port: Int, parallelism: Int, ckAndGroupID: String, topicName: String): Unit = {
    val conf = new Configuration()
    conf.setInteger("rest.port", port)

    // 1. 构建flink环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(parallelism)
//    env.getConfig.setAutoWatermarkInterval(200)

    val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
      .setGroupId(ckAndGroupID)
      .setTopics(topicName)
      .setBootstrapServers("bigdata1:9092,bigdata2:9092,bigdata3:9092")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), ckAndGroupID)

    // 2. 对数据进行处理
    handle(env, stream)

    // 3 执行环境
    env.execute()
  }

  def handle(env: StreamExecutionEnvironment, stream: DataStream[String]): Unit
}
