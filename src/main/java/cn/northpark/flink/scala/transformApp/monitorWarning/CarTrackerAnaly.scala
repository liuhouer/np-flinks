package cn.northpark.flink.scala.transformApp.monitorWarning

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.Properties

import cn.northpark.flink.scala.transformApp.util._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer

/**
  *  违法车辆轨迹保存到Hbase中
  *  1.从数据库mysql中找到对应的违法车辆数据
  *  2.将这些车辆的每条数据保存到HBase分布式数据库中
  */
object CarTrackerAnaly {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //Kafka 配置
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","group1126")

    //读取kafka 中监控到的车流量数据
    val lines: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("monitortopic1125",new SimpleStringSchema(),props).setStartFromEarliest())
    //对监控到车流量的数据进行转换
    val monitorCarInfoDS: DataStream[MonitorCarInfo] = lines.map(line => {
      val arr: Array[String] = line.split("\t")
      MonitorCarInfo(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5), arr(6).toDouble)
    })

    //从mysql中读取对应的违法车辆数据，设置广播流
    val violationCarInfoDS = env.addSource(new RichSourceFunction[ViolationCarInfo] {
      var conn:Connection = _
      var pst:PreparedStatement = _
      var stop = false

      //初始化 RichSourceFunction时执行一次
      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://192.168.179.14:3306/traffic_monitor","root","123456")
        pst = conn.prepareStatement("select car,violation,create_time,detail from t_violation_list_copy")
      }

      //一般设置循环周期性产生数据
      override def run(ctx: SourceFunction.SourceContext[ViolationCarInfo]): Unit = {
        while(!stop){
          val set: ResultSet = pst.executeQuery()
          while(set.next()){
            val car: String = set.getString(1)
            val violation: String = set.getString(2)
            val createTime: String = set.getString(3)
            val detail: String = set.getString(4)
            ctx.collect(ViolationCarInfo(car,violation,createTime,detail))
          }

          //每个半小时读取mysql中的数据
          Thread.sleep(1000*60*30)
        }
      }

      //当取消Flink任务时执行的方法
      override def cancel(): Unit = {
        pst.close()
        conn.close()
        stop = true
      }
    })

    //将读取到的 violationCarInfoDS 违法车辆数据流广播出去
    val mapDesc = new MapStateDescriptor[String,ViolationCarInfo]("mapDesc",classOf[String],classOf[ViolationCarInfo])
    val bcS: BroadcastStream[ViolationCarInfo] = violationCarInfoDS.broadcast(mapDesc)

    //将监控的车辆流与广播流通过connect连接
    val resultDS: DataStream[MonitorCarInfo] = monitorCarInfoDS.connect(bcS).process(new BroadcastProcessFunction[MonitorCarInfo, ViolationCarInfo, MonitorCarInfo] {
      //处理监控到的数据流车辆数据
      override def processElement(value: MonitorCarInfo, ctx: BroadcastProcessFunction[MonitorCarInfo, ViolationCarInfo, MonitorCarInfo]#ReadOnlyContext, out: Collector[MonitorCarInfo]): Unit = {
        val robs: ReadOnlyBroadcastState[String, ViolationCarInfo] = ctx.getBroadcastState(mapDesc)
        if (robs.contains(value.car)) {
          out.collect(value)
        }
      }

      //处理的违法车辆数据
      override def processBroadcastElement(value: ViolationCarInfo, ctx: BroadcastProcessFunction[MonitorCarInfo, ViolationCarInfo, MonitorCarInfo]#Context, out: Collector[MonitorCarInfo]): Unit = {
        val bs: BroadcastState[String, ViolationCarInfo] = ctx.getBroadcastState(mapDesc)
        bs.put(value.car, value)
      }
    })


    //对 数据转换成Put类型数据
    val putDs: DataStream[Put] = resultDS.map(mci => {
      val car: String = mci.car
      val monitorId: String = mci.monitorId
      val time: Long = mci.actionTime
      val put = new Put(Bytes.toBytes(car + "_" + time))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("car"), Bytes.toBytes(car))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("monitorId"), Bytes.toBytes(monitorId))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("time"), Bytes.toBytes(time))
      put
    })
    //使用CountWindow 操作，将数据写入到HBase
    putDs.countWindowAll(10).process(new ProcessAllWindowFunction[Put,util.List[Put], GlobalWindow] {
      override def process(context: Context, elements: Iterable[Put], out: Collector[util.List[Put]]): Unit = {
        val list = new util.ArrayList[Put]()
        val iter: Iterator[Put] = elements.iterator
        while (iter.hasNext) {
          val put: Put = iter.next()
          list.add(put)
        }
        out.collect(list)
      }
    }).addSink(new HBaseSink())


    //将数据保存到HBase
//      .addSink(new RichSinkFunction[MonitorCarInfo] {
//      var configuration: conf.Configuration = _
//      var conn: client.Connection = _
//      //初始化RichSinkFunction 执行一次
//      override def open(parameters: Configuration): Unit = {
//        configuration = HBaseConfiguration.create()
//        configuration.set("hbase.zookeeper.quorum","node3:2181,node4:2181,node5:2181")
//        conn = ConnectionFactory.createConnection(configuration)
//      }
//
//      override def invoke(value: MonitorCarInfo, context: SinkFunction.Context[_]): Unit = {
//        val car: String = value.car
//        val time: Long = value.actionTime
//        val monitorId: String = value.monitorId
//        //连接HBase 表
//        val table: Table = conn.getTable(TableName.valueOf("a1"))
//
//        //准备put对象
//        val put = new Put(Bytes.toBytes(car+"_"+time))
//        //put 对象设置列
//        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("car"),Bytes.toBytes(car))
//        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("time"),Bytes.toBytes(time))
//        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("monitorId"),Bytes.toBytes(monitorId))
//        //当前此条数据插入hbase
//        table.put(put)
//      }
//    })

    env.execute()

  }

}
