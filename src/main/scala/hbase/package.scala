import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

package object hbase {
  val conn: Connection = ConnectionFactory.createConnection()
}
