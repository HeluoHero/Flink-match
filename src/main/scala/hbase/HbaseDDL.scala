package hbase

import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{NamespaceDescriptor, TableName}

import java.io.IOException

object HbaseDDL {

  /**
   * 创建命名空间
   *
   * @param namespace 命名空间名称
   */
  def createNameSpace(namespace: String): Unit = {
    // 1.获取admin admin的连接是轻量级的 不是线程安全的 不推荐池化或者缓存这个连接
    val admin = conn.getAdmin

    // 2.调用方法创建命名空间
    // 代码相对shell更加底层 所以shell能实现的功能 代码一定能实现
    // 所以需要填写完整的命名空间描述

    // 2.1 创建命名空间描述建造者
    val builder = NamespaceDescriptor.create(namespace)

    // 2.2 给命名空间添加需求
    //    builder.addConfiguration("user", "root")

    // 2.3 使用builder构造对应的添加完参数的对象 完成创建
    // 创建命名空间出现的问题 都属于本方法自身的问题 不应该抛出异常
    try {
      admin.createNamespace(builder.build())
    } catch {
      case e: IOException =>
        println(s"${namespace}已经存在")
        e.printStackTrace()
    }

    // 关闭admin
    admin.close()
  }

  /**
   * 判断表格是否存在
   *
   * @param namespace 命名空间名称
   * @param tableName 表格名称
   * @return true表示存在，false表示不存在
   */
  def isTableExists(namespace: String, tableName: String): Boolean = {
    val admin = conn.getAdmin

    val bool: Boolean = try {
      admin.tableExists(TableName.valueOf(namespace, tableName))
    } catch {
      case e: IOException =>
        e.printStackTrace()
        false
    }
    admin.close()
    bool
  }

  /**
   * 创建表格
   *
   * @param namespace    命名空间名称
   * @param tableName    表格名称
   * @param columnFamily 列族名称 可以有多个
   */
  def createTable(namespace: String, tableName: String, columnFamily: String*): Unit = {
    // 判断是否有一个列族
    if (columnFamily.isEmpty) {
      println("创建表格至少需要一个列族")
      return
    }

    if (isTableExists(namespace, tableName)) {
      println(s"$namespace:$tableName 表格已经存在")
      return
    }

    // 1.获取admin
    val admin = conn.getAdmin

    // 2. 调用方法创建表格
    // 2.1 创建表格描述的建造者
    val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName))

    // 2.2 添加参数
    columnFamily.foreach(e => {
      // 2.3 创建列族描述
      val bytes = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(e))

      // 2.4 对应当前的列族添加参数
      // 添加版本参数
      bytes.setMaxVersions(5)

      // 2.5 创建添加完参数的列族描述
      tableDescriptorBuilder.setColumnFamily(bytes.build())
    })


    // 2.6 创建对应表格的描述
    try {
      admin.createTable(tableDescriptorBuilder.build())
    } catch {
      case e: IOException =>
        println(s"${tableName}表格已经存在")
        e.printStackTrace()
    }

    admin.close()
  }

  /**
   * 修改表格中的一个列族的版本
   *
   * @param namespace    命名空间名称
   * @param tableName    表格名称
   * @param columnFamily 列族名称
   * @param version      版本号
   */
  def modifyTable(namespace: String, tableName: String, columnFamily: String, version: Int): Unit = {
    // 判断表格是否存在
    if (!isTableExists(namespace, tableName)) {
      println(s"$namespace:$tableName 表格不存在")
      return
    }

    // 1. 获取admin
    val admin = conn.getAdmin

    try {
      // 2. 调用方法修改表格
      // 2.0 获取之前的表格描述
      val descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName))

      // 2.1 创建一个表格描述
      // 如果使用填写的tableName的方法 相当于创建了一个新的表格描述建造者 没有之前的信息
      // 如果想要修改之前的信息 必须调用方法填写一个旧的表格描述
      val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor)

      // 2.2 对应建造者进行表格的修改
      val bytes1 = descriptor.getColumnFamily(Bytes.toBytes(columnFamily))

      // 创建列族描述建造者
      //需要填写旧的列族描述
      val bytes = ColumnFamilyDescriptorBuilder.newBuilder(bytes1)

      // 修改对应版本
      bytes.setMaxVersions(version)

      // 此处修改的时候 如果填写的是新创建的 那么别的参数会初始化
      tableDescriptorBuilder.modifyColumnFamily(bytes.build())

      admin.modifyTable(tableDescriptorBuilder.build())
    } catch {
      case e: IOException => e.printStackTrace()
    }

    // 3. 关闭admin
    admin.close()
  }

  /**
   * 删除表格
   *
   * @param namespace 命名空间名称
   * @param tableName 表格名称
   * @return true表示删除成功
   */
  def deleteTable(namespace: String, tableName: String): Boolean = {
    // 1. 判断表格是否存在
    if (!isTableExists(namespace, tableName)) {
      println(s"$namespace:$tableName 已经存在")
      return false
    }

    // 2. 获取admin
    val admin = conn.getAdmin

    // 3. 调用相关的方法删除表格
    try {
      // HBase删除表格之前 一定要先标记表格为不可用
      val tableName1 = TableName.valueOf(namespace, tableName)
      admin.disableTable(tableName1)
      admin.deleteTable(tableName1)
    } catch {
      case e: IOException => e.printStackTrace()
    }

    admin.close()
    true
  }

  def main(args: Array[String]): Unit = {
    // 测试创建命名空间
    // 应该先保证连接没有问题 再来调用相关的方法
    //    createNameSpace("school")

    // 测试判断表格是否存在
    //println(isTableExists("bigdata", "aaa"))

    // 测试创建表格
    //    createTable("bigdata", "person", "ssa")

    // 测试修改表格
    //    modifyTable("bigdata", "student", "info", 6)

    // 测试删除表格
    deleteTable("bigdata", "person")

    println("=" * 20)

    conn.close()
  }
}
