package com.heluo

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions}
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

package object match4 {
  // 298,110,0000,2024-04-07 13:55:21,2024-04-07 13:55:24,2024-04-07 13:55:27,11560,1900-01-01 00:00:00,184962,1
  case class ProduceRecord(id: String, status: String)
  case class ChangeRecord(id: String, status: String, changeStartTime: Long, check: Int)

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

  val jdbcExecutionOptions: JdbcExecutionOptions = JdbcExecutionOptions.builder()
    .withMaxRetries(3)
    .withBatchSize(100)
    .withBatchIntervalMs(3000)
    .build()
}
