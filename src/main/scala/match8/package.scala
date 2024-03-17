
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig


package object match8 {
  case class Environment(id: String, degree: Double, create_time: String)
  case class Change(id: String, status: String)

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  def getKafkaConf(topic: String): KafkaSource[String] = KafkaSource.builder[String]()
    .setGroupId(topic)
    .setTopics(topic)
    .setBootstrapServers("bigdata1:9092")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build()

  val redisConf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
    .setHost("bigdata1")
    .setPort(6379)
    .build()

  val mysqlConf: JdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    .withUrl("jdbc:mysql://bigdata1:3306/shtd_industry?useSSL=false&characterEncoding=UTF-8")
    .withDriverName("com.mysql.jdbc.Driver")
    .withUsername("root")
    .withPassword("123456")
    .withConnectionCheckTimeoutSeconds(60)
    .build()

  val jdbcExceptionOptions: JdbcExecutionOptions = JdbcExecutionOptions.builder()
    .withMaxRetries(3)
    .withBatchSize(100)
    .withBatchIntervalMs(3000)
    .build()
}
