package cn.northpark.flink.scala.transformApp.monitorWarning

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.Properties

import cn.northpark.flink.scala.transformApp.util._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * 如果一辆车在5分钟内连续超速通过3个卡扣，这辆车认为是危险驾驶
  */
object DangerDriverInfo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置Kafka 配置
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringSerializer].getName)
    props.setProperty("value.deserializer",classOf[StringSerializer].getName)
    props.setProperty("group.id","group112505")
    //创建Kafka Source
    val kafkaSource: FlinkKafkaConsumerBase[String] = new FlinkKafkaConsumer[String]("monitortopic1125",new SimpleStringSchema(),props).setStartFromEarliest()
    //读取车辆监控信息
    val ds1: DataStream[String] = env.addSource(kafkaSource)
//        val ds1: DataStream[String] = env.socketTextStream("node5",9999)

    //对车辆监控信息进行转换数据
    val carMonitorDS: DataStream[MonitorCarInfo] = ds1.map(line => {
      val arr: Array[String] = line.split("\t")
      MonitorCarInfo(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5), arr(6).toDouble)
    })
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MonitorCarInfo](Time.seconds(5)) {
//      override def extractTimestamp(element: MonitorCarInfo): Long = element.actionTime
//    })

    //对以上每条数据进行过滤找出，超速的车辆信息流
    //1.定义事件流
    val overSpeedCarInfoDS: DataStream[NewMonitorCarInfo] = carMonitorDS.map(new RichMapFunction[MonitorCarInfo, NewMonitorCarInfo] {
      var conn: Connection = _
      var pst: PreparedStatement = _
      var stop = false

      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://192.168.179.14:3306/traffic_monitor", "root", "123456")
        pst = conn.prepareStatement("select " +
          "area_id,road_id,monitor_id,speed_limit " +
          "from t_monitor_speed_limit_info " +
          "where area_id =? and road_id = ? and monitor_id = ? ")
      }

      override def map(value: MonitorCarInfo): NewMonitorCarInfo = {
        val areaId: String = value.areaId
        val roadId: String = value.roadId
        val monitorId: String = value.monitorId
        val cameraId: String = value.cameraId
        val actionTime: Long = value.actionTime
        pst.setString(1, areaId)
        pst.setString(2, roadId)
        pst.setString(3, monitorId)
        val set: ResultSet = pst.executeQuery()
        var limitSpeed: Double = 10.0
        while (set.next()) {
          limitSpeed = set.getDouble("speed_limit")
        }
        NewMonitorCarInfo(areaId, roadId, monitorId, cameraId, actionTime, value.car, value.speed, limitSpeed)
      }

      override def close(): Unit = {
        stop = true
        pst.close()
        conn.close()
      }
    })

    //2.定义模式
    val pattern: Pattern[NewMonitorCarInfo, NewMonitorCarInfo] = Pattern.begin[NewMonitorCarInfo]("first").where(nmci => {
      nmci.speed > nmci.speedLimit * 1.2
    })
      .followedBy("second").where(nmci => {
      nmci.speed > nmci.speedLimit * 1.2
    })
      .followedBy("third").where(nmci => {
      nmci.speed > nmci.speedLimit * 1.2
    })
      .within(Time.seconds(5))


          //3.事件流应用模式
          val ps: PatternStream[NewMonitorCarInfo] = CEP.pattern(overSpeedCarInfoDS.keyBy(_.car),pattern)

          //4.选择事件
          val violationCarInfoDS: DataStream[ViolationCarInfo] = ps.select(new PatternSelectFunction[NewMonitorCarInfo, ViolationCarInfo] {
          //pattern : 匹配上的事件
          override def select(pattern: util.Map[String, util.List[NewMonitorCarInfo]]): ViolationCarInfo = {
          val firstInfo: NewMonitorCarInfo = pattern.get("first").iterator().next()
          val secondInfo: NewMonitorCarInfo = pattern.get("second").iterator().next()
          val thirdInfo: NewMonitorCarInfo = pattern.get("third").iterator().next()
          val str = s"车辆${firstInfo.car}涉嫌危险驾驶，第一次经过卡扣-时间：${firstInfo.monitorId}-${firstInfo.actionTime}|" +
          s"第二次经过卡扣-时间：${secondInfo.monitorId}-${secondInfo.actionTime}|" +
          s"第三次经过卡扣-时间：${thirdInfo.monitorId}-${thirdInfo.actionTime}"

        ViolationCarInfo(firstInfo.car,
          "涉嫌危险驾驶",
          DateUtils.timestampToDataStr(System.currentTimeMillis()),
          str
        )
      }
    })
    //写入数据库
    violationCarInfoDS.print()
    violationCarInfoDS.addSink(new JDBCSink[ViolationCarInfo]("ViolationCarInfo"))
    env.execute()
  }

}
