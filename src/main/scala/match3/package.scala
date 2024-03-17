import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

package object match3 {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val kafkaConf: KafkaSource[String] = KafkaSource.builder[String]()
    .setGroupId("san")
    .setBootstrapServers("bigdata1:9092")
    .setTopics("order")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema)
    .build()

  val redisConf = new FlinkJedisPoolConfig.Builder()
    .setHost("bigdata1")
    .setPort(6379)
    .build()

}
