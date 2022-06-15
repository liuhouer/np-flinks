package cn.northpark.flink.scala.transformApp.util

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, client}
/**
  *  HBase sink ,批量数据插入到HBase中
  */
class HBaseSink extends RichSinkFunction[java.util.List[Put]] {
  var configuration: conf.Configuration = _
  var conn: client.Connection = _
  //初始化 RichSinkFunction 对象时 执行一次
  override def open(parameters: Configuration): Unit = {
    configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum","node3:2181,node4:2181,node5:2181")
    conn = ConnectionFactory.createConnection(configuration)
  }


  override def invoke(value: util.List[Put], context: SinkFunction.Context): Unit = {
    //连接HBase 表
    val table: Table = conn.getTable(TableName.valueOf("a1"))
    table.put(value)
  }
}
