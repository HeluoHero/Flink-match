package com.heluo

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions}
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

package object match8 {
  case class EnvironmentData(id: String, temperature: Double)
  case class ChangeRecord(id: String, status: String, check: Int)

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

  val jdbcExecutionOptions: JdbcExecutionOptions = new JdbcExecutionOptions.Builder()
    .withMaxRetries(3)
    .withBatchSize(100)
    .withBatchIntervalMs(3000)
    .build()
}
