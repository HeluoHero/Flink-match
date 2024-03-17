package hbase

import org.apache.hadoop.hbase.{CellUtil, CompareOperator, TableName}
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Delete, Get, Put, Scan}
import org.apache.hadoop.hbase.filter.{ColumnValueFilter, FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes

import java.io.IOException

object HBaseDML {

  /**
   * 插入数据
   *
   * @param namespace    命名空间名称
   * @param tableName    表格名称
   * @param rowKey       主键
   * @param columnFamily 列族
   * @param columnName   列名
   * @param value        值
   */
  def putCell(namespace: String, tableName: String, rowKey: String, columnFamily: String, columnName: String, value: String): Unit = {
    // 1.获取table
    val table = conn.getTable(TableName.valueOf(namespace, tableName))

    // 2. 调用相关方法插入数据
    // 2.1 创建put对象
    val put = new Put(Bytes.toBytes(rowKey))

    // 2.2 给put对象添加数据
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value))

    // 2.3 将对象写入对应的方法
    try {
      table.put(put)
    } catch {
      case e: IOException => e.printStackTrace()
    }

    // 3.关闭table
    table.close()
  }

  def putCell2(namespace: String, tableName: String, rowKey: String, columnFamily: String, columnName: String, value: String): Unit = {
    var mutator: BufferedMutator = null
    // mutator = conn.getBufferedMutator(TableName.valueOf(namespace, tableName));
    val params = new BufferedMutatorParams(TableName.valueOf(namespace, tableName))
    params.writeBufferSize(5 * 1024 * 1024) // 当文件数据量大于5MB，则停止写入
    params.setWriteBufferPeriodicFlushTimeoutMs(3000L) // 当写入时间超过三秒，停止写入
    mutator = conn.getBufferedMutator(params)

    val put = new Put(Bytes.toBytes(rowKey))

    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value))

    mutator.mutate(put)

    if (mutator != null) mutator.close()
  }

  /**
   * 读取对应的一行中的某一行
   *
   * @param namespace    命名空间名称
   * @param tableName    表格名称
   * @param rowKey       主键
   * @param columnFamily 列族
   * @param columnName   列名
   */
  def getCell(namespace: String, tableName: String, rowKey: String, columnFamily: String, columnName: String): Unit = {
    // 1. 获取table
    val table = conn.getTable(TableName.valueOf(namespace, tableName))

    // 2. 创建get对象
    val get = new Get(Bytes.toBytes(rowKey))

    // 如果直接调用get方法读取数据 此时一整行数据
    // 如果想读取某一列的数据 需要添加对应的参数
    get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))

    // 设置读取数据的版本
    get.readAllVersions()

    try {
      val result = table.get(get)

      val cells = result.rawCells()

      // 测试方法：直接把读取的数据打印到控制台
      // 如果是实际开发 需要再额外写方法 对应处理数据
      cells.foreach(e => println(new String(CellUtil.cloneValue(e))))
    } catch {
      case e: IOException => e.printStackTrace()
    }

    table.close()
  }

  /**
   * 扫描数据
   *
   * @param namespace 命名空间
   * @param tableName 表格名称
   * @param startRow  开始的row 包会的
   * @param stopRow   结束的row 不包会的
   */
  def scanRows(namespace: String, tableName: String, startRow: String, stopRow: String): Unit = {
    // 1. 获取table
    val table = conn.getTable(TableName.valueOf(namespace, tableName))

    // 2. 创建scan对象
    val scan = new Scan()
    // 如果此时直接调用 会直接扫描

    // 添加参数 来控制扫描的数据
    scan.withStartRow(Bytes.toBytes(startRow))
    scan.withStopRow(Bytes.toBytes(stopRow))

    try {
      // 读取多行数据 获得scanner
      val scanner = table.getScanner(scan)

      // result来记录一行数据 cell数据
      // ResultScanner来记录多行数据 result的数组
      scanner.forEach(e => {
        e.rawCells().foreach(cell => {
          print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneValue(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "\t")
        })
        println()
      })
    } catch {
      case e: IOException => e.printStackTrace()
    }

    table.close()
  }

  /**
   * 扫描数据
   *
   * @param namespace    命名空间
   * @param tableName    表格名称
   * @param startRow     开始的row 包会的
   * @param stopRow      结束的row 不包会的
   * @param columnFamily 列族
   * @param columnName   列名
   * @param value        值
   */
  def filterRows(namespace: String, tableName: String, startRow: String, stopRow: String, columnFamily: String, columnName: String, value: String): Unit = {
    // 1. 获取table
    val table = conn.getTable(TableName.valueOf(namespace, tableName))

    // 2. 创建scan对象
    val scan = new Scan()
    // 如果此时直接调用 会直接扫描

    // 添加参数 来控制扫描的数据
    scan.withStartRow(Bytes.toBytes(startRow))
    scan.withStopRow(Bytes.toBytes(stopRow))

    // 可以添加多个过滤
    val filterList = new FilterList()

    // 创建过滤器
    // （1）结果只保留当前列的数据
    val columnValueFilter = new ColumnValueFilter(
      Bytes.toBytes(columnFamily), // 列族名称
      Bytes.toBytes(columnName), // 列名
      CompareOperator.EQUAL, // 比较关系 此处为等于
      Bytes.toBytes(value) // 值
    )

    // （2）结果保留整行数据
    // 结果同时会保留没有当前的数据
    val singleColumnValueFilter = new SingleColumnValueFilter(
      Bytes.toBytes(columnFamily), // 列族名称
      Bytes.toBytes(columnName), // 列名
      CompareOperator.EQUAL, // 比较关系 此处为等于
      Bytes.toBytes(value) // 值
    )

    filterList.addFilter(singleColumnValueFilter)

    scan.setFilter(filterList)

    try {
      // 读取多行数据 获得scanner
      val scanner = table.getScanner(scan)

      // result来记录一行数据 cell数据
      // ResultScanner来记录多行数据 result的数组
      scanner.forEach(e => {
        e.rawCells().foreach(cell => {
          print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)) + "\t")
        })
        println()
      })
    } catch {
      case e: IOException => e.printStackTrace()
    }

    table.close()
  }

  /**
   * 删除一行中的一列数据
   *
   * @param namespace    命名空间名称
   * @param tableName    表格名称
   * @param rowKey       主键
   * @param columnFamily 列族
   * @param columnName   列名
   */
  def deleteColumn(namespace: String, tableName: String, rowKey: String, columnFamily: String, columnName: String): Unit = {
    // 1.获取table
    val table = conn.getTable(TableName.valueOf(namespace, tableName))

    // 2.创建delete对象
    val delete = new Delete(Bytes.toBytes(rowKey))

    // 添加列信息
    // addColumn删除一个版本
    // addColumns删除全部版本
    // 按照逻辑需要删除所有版本的数据
    delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))

    try {
      table.delete(delete)
    } catch {
      case e: IOException => e.printStackTrace()
    }

    table.close()
  }


  def main(args: Array[String]): Unit = {
    //    putCell("bigdata", "student",  "2001", "info", "name", "张三")
    //    putCell2("bigdata", "student", "2001", "info", "name", "李四")

    getCell("bigdata", "student", "2001", "info", "name")

    //    filterRows("bigdata", "student", "1001", "1004", "info", "name", "lisi")

    deleteColumn("bigdata", "student", "2001", "info", "name")

    println("=" * 25)

    conn.close()
  }
}
