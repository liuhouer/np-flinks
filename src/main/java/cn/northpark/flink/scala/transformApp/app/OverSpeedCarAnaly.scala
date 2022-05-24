package cn.northpark.flink.scala.transformApp.app

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import cn.northpark.flink.scala.transformApp.util._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer

/**
  *  车辆超速信息
  *  统计 车辆超速20%车辆，保存到mysql结果表中
  *
  *  注意：使用广播数据流，使用场景，两个流要关联，一个流大，一个流小，小的流会定期变化，可以将小的流全量广播到每个taskManager中。
  */
object OverSpeedCarAnaly {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //设置Kafka 配置
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringSerializer].getName)
    props.setProperty("value.deserializer",classOf[StringSerializer].getName)
    props.setProperty("group.id","group1125")
    //创建Kafka Source
    val kafkaSource: FlinkKafkaConsumerBase[String] = new FlinkKafkaConsumer[String]("monitortopic1125",new SimpleStringSchema(),props).setStartFromEarliest()
    //读取车辆监控信息
    val ds1: DataStream[String] = env.addSource(kafkaSource)
//    val ds1: DataStream[String] = env.socketTextStream("node5",9999)

    //对车辆监控信息进行转换数据
    val carMonitorDS: DataStream[MonitorCarInfo] = ds1.map(line => {
      val arr: Array[String] = line.split("\t")
      MonitorCarInfo(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5), arr(6).toDouble)
    })

    //读取区域-道路-卡扣 车辆限速信息，读取mysql中的数据，每隔半小时读取一次
    val limitSpeedDs: DataStream[MonitorLimitSpeedInfo] = env.addSource(new RichSourceFunction[MonitorLimitSpeedInfo] {

      var conn: Connection = _
      var pst: PreparedStatement = _
      var stop = false

      //当初始化 RichSourceFunction时调用一次，可以初始化数据库连接对象
      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://192.168.179.14:3306/traffic_monitor", "root", "123456")
        pst = conn.prepareStatement("select area_id,road_id,monitor_id,speed_limit from t_monitor_speed_limit_info")
      }

      //产生数据的方法，一般使用一个循环来实现读取mysql数据
      override def run(ctx: SourceFunction.SourceContext[MonitorLimitSpeedInfo]): Unit = {
        while (!stop) {
          val set: ResultSet = pst.executeQuery()
          while (set.next()) {
            val areaId: String = set.getString("area_id")
            val roadId: String = set.getString(2)
            val monitorId: String = set.getString(3)
            val speedLimit: Double = set.getDouble(4)
            ctx.collect(new MonitorLimitSpeedInfo(areaId, roadId, monitorId, speedLimit))
          }

          //每隔半小时查询一次数据库全量的限速信息
          Thread.sleep(30 * 1000 * 60)
        }
      }

      //当Flink程序取消时，自动调用cancel方法
      override def cancel(): Unit = {
        stop = true
        pst.close()
        conn.close()
      }
    })

    //将从mysql中读取的限速信息，使用广播流广播出去
    val mapStateDesc = new MapStateDescriptor[String,Double]("map-state",classOf[String],classOf[Double])
    val bcLimitSpeed: BroadcastStream[MonitorLimitSpeedInfo] = limitSpeedDs.broadcast(mapStateDesc)

    //连接车辆监控的数据流与当前的广播流 ,进行车辆超速判断
    val overSpeedInfoDs: DataStream[OverSpeedCarInfo] = carMonitorDS.connect(bcLimitSpeed).process(new BroadcastProcessFunction[MonitorCarInfo, MonitorLimitSpeedInfo, OverSpeedCarInfo] {
      //针对车辆监控数据进行处理，value : 当前本条数据 cxt：Flink上下文，out：回收数据对象
      override def processElement(value: MonitorCarInfo, ctx: BroadcastProcessFunction[MonitorCarInfo, MonitorLimitSpeedInfo, OverSpeedCarInfo]#ReadOnlyContext, out: Collector[OverSpeedCarInfo]): Unit = {
        val areaId: String = value.areaId
        val monitorId: String = value.monitorId
        val cameraId: String = value.cameraId
        //本条数据区域_道路_卡扣 信息
        val key = areaId + "_" + monitorId + "_" + cameraId
        //获取广播状态，判断当前数据key 是否有对应的限速状态
        val readOnlyBS: ReadOnlyBroadcastState[String, Double] = ctx.getBroadcastState(mapStateDesc)
        if (readOnlyBS.contains(key)) {
          //如果包含当前key对应的限速
          if (value.speed > readOnlyBS.get(key) * 1.2) {
            //超速车辆，回收回去
            out.collect(new OverSpeedCarInfo(value.car, value.monitorId, value.roadId, value.speed, readOnlyBS.get(key), value.actionTime))
          }
        } else {
          //如果不包含限速信息，默认限速为60
          if (value.speed > 60 * 1.2) {
            //超速车辆信息，回收回去
            out.collect(new OverSpeedCarInfo(value.car, value.monitorId, value.roadId, value.speed, 60.0, value.actionTime))
          }
        }
      }

      //针对广播流数据进行处理 ，value :当前广播的此条数据，ctx :Flink上下文，out：回收数据对象
      override def processBroadcastElement(value: MonitorLimitSpeedInfo, ctx: BroadcastProcessFunction[MonitorCarInfo, MonitorLimitSpeedInfo, OverSpeedCarInfo]#Context, out: Collector[OverSpeedCarInfo]): Unit = {
        val broadCastState: BroadcastState[String, Double] = ctx.getBroadcastState(mapStateDesc)
        val key = value.areaId + "_" + value.roadId + "_" + value.monitorId //key : 区域_道路_卡扣
        broadCastState.put(key, value.limitSpeed)
      }
    })

    //将结果写入mysql 表 t_speed_info
    overSpeedInfoDs.addSink(new JDBCSink[OverSpeedCarInfo]("OverSpeedCarInfo"))

    env.execute()
  }

}
