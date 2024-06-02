package com.heluo.match8.comment

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

trait BaseApp {
  def start(ckAndGroupID: String, topicName: String): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
      .setGroupId(ckAndGroupID)
      .setTopics(topicName)
      .setBootstrapServers("bigdata1:9092,bigdata2:9092,bigdata3:9092")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "")

    handle(stream)

    env.execute()
  }

  def handle(stream: DataStream[String]): Unit
}
