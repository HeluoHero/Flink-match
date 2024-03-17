import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

package object match4 {

  case class ProduceRecord(produceID: String, changeHandleState: String)
  case class ChangeRecord(id: String, status: String, change_start_time: String, check: Int)

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  def getKafkaConf(topic: String): KafkaSource[String] = KafkaSource.builder[String]()
    .setGroupId(topic)
    .setTopics(topic)
    .setBootstrapServers("bigdata1:9092")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema)
    .build()

  val redisConf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
    .setHost("bigdata1")
    .setPort(6379)
    .build()

  val mysqlSink: JdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    .withUrl("jdbc:mysql://bigdata1:3306/shtd_industry?useSSL=false")
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
