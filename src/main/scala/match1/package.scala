import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

package object match1 {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)

  val kafkaSet: KafkaSource[String] = KafkaSource.builder[String]
    .setBootstrapServers("bigdata1:9092") // 指定kafka节点的地址和端口
    .setGroupId("consumer-group") // 指定消费组的id
    .setTopics("order") // 指定消费者的主题（topic）
    .setStartingOffsets(OffsetsInitializer.latest()) // flink消费kafka的策略
    .setValueOnlyDeserializer(new SimpleStringSchema()) // 指定 反序列化器，这个是反序列化value
    .build()

  val redisConf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
    .setHost("bigdata1")
    .setPort(6379)
    .build()

  val mysqlConf: JdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    .withUrl("jdbc:mysql://bigdata1:3306/shtd_result?useSSL=false&characterEncoding=UTF-8")
    .withDriverName("com.mysql.jdbc.Driver")
    .withUsername("root")
    .withPassword("123456")
    .withConnectionCheckTimeoutSeconds(60) // 超时60秒如果无法连接msyql
    .build()

  val jdbcExecutionOptions: JdbcExecutionOptions = JdbcExecutionOptions.builder()
    .withMaxRetries(3) // 重试3次
    .withBatchSize(100)
    .withBatchIntervalMs(3000) // 与withBatchSize是符合其中一个就
    .build()

}
