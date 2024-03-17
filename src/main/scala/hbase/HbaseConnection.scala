package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.{AsyncConnection, Connection, ConnectionFactory}

import java.util.concurrent.CompletableFuture

object HbaseConnection {

//  private var conn: Connection = ConnectionFactory.createConnection()

  /*private val conf = {
    val configuration = HBaseConfiguration.create()
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "hadoop102,hadoop103,hadoop104")
    configuration
  }*/


  def main(args: Array[String]): Unit = {

    // 1.创建连接配置对象
//    val conf = new Configuration()

    // 2.添加配置参数
//    conf.set(HConstants.ZOOKEEPER_QUORUM, "bigdata1,bigdata2,bigdata3")

    // 3.创建连接
    // 默认使用同步连接
//    val connection = ConnectionFactory.createConnection()

    // 可以使用异步连接
//    val asyncConnection: CompletableFuture[AsyncConnection] = ConnectionFactory.createAsyncConnection(conf)

    // 使用连接
    println(conn)
    conn.close()
  }
}
