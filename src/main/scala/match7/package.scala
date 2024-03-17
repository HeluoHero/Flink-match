import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

package object match7 {
  case class OrderInfo(id: String, final_total_amount: Double, status: String, create_time: String, operate_time: String)
  case class OrderDetail(orderID: String,sku_id:String, price: Double, sku_num: Int, create_time: String)

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  def getKafkaConf(topic: String): KafkaSource[String] = KafkaSource.builder[String]()
    .setGroupId(topic)
    .setTopics(topic)
    .setBootstrapServers("bigdata1:9092")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build()

  val redisConf: FlinkJedisPoolConfig  = new FlinkJedisPoolConfig.Builder()
    .setHost("bigdata1")
    .setPort(6379)
    .build()



}
