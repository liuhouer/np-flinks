package cn.northpark.flink.scala.transformApp.monitorWarning

import java.util.Properties

import cn.northpark.flink.scala.transformApp.util._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

/**
  *  实时对违法车辆-套牌车辆 进行监控
  */
object RepatitionCarInfo {
  def main(args: Array[String]): Unit = {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //设置kafka 配置
    val props = new Properties()
    props.setProperty("bootstrap.servers","mynode1:9092,mynode2:9092,mynode3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","group112504")

    //Flink 读取Kafka 中数据
    val carMonitorInfoDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("monitortopic1125",new SimpleStringSchema(),props).setStartFromEarliest())

    //转换数据
    val transferDs: DataStream[MonitorCarInfo] = carMonitorInfoDS.map(line => {
      val arr: Array[String] = line.split("\t")
      MonitorCarInfo(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5), arr(6).toDouble)
    })

    //对监控到车辆数据，使用状态编程 ，统计在10s 内通过两个卡扣的涉嫌套牌车辆
    val violationCarInfoDS: DataStream[ViolationCarInfo] = transferDs.keyBy(_.car).process(new KeyedProcessFunction[String, MonitorCarInfo, ViolationCarInfo] {

      //给每个车辆设置状态，状态中存储的是最近这辆车经过卡扣的时间
      private lazy val carStateTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("carTimeState", classOf[Long]))

      //value : 当前本条数据 ，ctx :上下文数据，out:回收数据对象
      override def processElement(value: MonitorCarInfo, ctx: KeyedProcessFunction[String, MonitorCarInfo, ViolationCarInfo]#Context, out: Collector[ViolationCarInfo]): Unit = {
        //获取当前车辆的状态值
        val preLastActionTime: Long = carStateTime.value()
        //判断当前车辆是否有对应的状态值
        if (preLastActionTime == 0) {
          //没有状态，存储状态
          carStateTime.update(value.actionTime)
        } else {
          //获取当前数据通过卡扣拍摄时间
          val actionTime: Long = value.actionTime
          val car: String = value.car
          //有对应的状态，就获取状态时间与当前本条数据时间做差值，看下是否超过10s
          if ((actionTime - preLastActionTime).abs < 10 * 1000) {
            //涉嫌套牌车辆
            out.collect(new ViolationCarInfo(car,
              "涉嫌套牌",
              DateUtils.timestampToDataStr(System.currentTimeMillis()),
              s"车辆${car}通过上一次卡扣时间${preLastActionTime},本次通过卡扣时间${actionTime},两次时间差值为${(preLastActionTime - actionTime).abs}ms"))
          }
          //更新状态
          carStateTime.update(actionTime.max(preLastActionTime))
        }

      }
    })

    violationCarInfoDS.addSink(new JDBCSink[ViolationCarInfo]("ViolationCarInfo"))
    env.execute()


  }

}
