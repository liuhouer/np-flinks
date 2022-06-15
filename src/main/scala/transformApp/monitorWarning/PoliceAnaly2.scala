package cn.northpark.flink.scala.transformApp.monitorWarning

import cn.northpark.flink.scala.transformApp.util._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  *  使用状态编程+定时器来解决 违法-出警问题
  */
object PoliceAnaly2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //获取违法信息
    val violationInfoDS: DataStream[String] = env.socketTextStream("node5",9999)

    //获取出警信息
    val policeInfoDS: DataStream[String] = env.socketTextStream("node5",8888)

    //对违法车辆数据进行转换
    val violationTransferDS: DataStream[ViolationCarInfo] = violationInfoDS.map(line => {
      val arr: Array[String] = line.split(",")
      ViolationCarInfo(arr(0), arr(1), arr(2), arr(3))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ViolationCarInfo](Time.seconds(0)) {
      override def extractTimestamp(element: ViolationCarInfo): Long = element.createTime.toLong
    })

    //对出警信息进行转换
    val policeTransferDS: DataStream[PoliceInfo] = policeInfoDS.map(line => {
      val arr: Array[String] = line.split(",")
      PoliceInfo(arr(0), arr(1), arr(2).toLong, arr(3))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PoliceInfo](Time.seconds(0)) {
      override def extractTimestamp(element: PoliceInfo): Long = element.policeTime
    })

    //将以上两个流通过connect合并，来进行处理
    val resultDs: DataStream[String] = violationTransferDS.keyBy(_.car).connect(policeTransferDS.keyBy(_.car))
      .process(new KeyedCoProcessFunction[String, ViolationCarInfo, PoliceInfo, String] {
        //设置出警状态
        private lazy val policeInfoState: ValueState[PoliceInfo] = getRuntimeContext.getState(new ValueStateDescriptor[PoliceInfo]("PoliceInfoState", classOf[PoliceInfo]))

        //设置违法状态
        private lazy val violationCarInfoState: ValueState[ViolationCarInfo] = getRuntimeContext.getState(new ValueStateDescriptor[ViolationCarInfo]("ViolationCarInfo", classOf[ViolationCarInfo]))

        //处理 违法车辆信息 ，value : 当前这条数据，ctx:Flink 上下文，out:回收数据对象
        override def processElement1(value: ViolationCarInfo, ctx: KeyedCoProcessFunction[String, ViolationCarInfo, PoliceInfo, String]#Context, out: Collector[String]): Unit = {
          val info: PoliceInfo = policeInfoState.value()
          if (info != null) {
            //当前车辆已经有出警信息
            out.collect(s"${value.car}已经出警，出警信息:${info}")
            //删除定时器
            ctx.timerService().deleteEventTimeTimer(info.policeTime + 5000)
            //将policeInfoState 状态中的值清空
            policeInfoState.clear()
          } else {
            //当前车辆没有出警信息，就设置定时器，当期时间延迟5s后执行的定时器
            ctx.timerService().registerEventTimeTimer(value.createTime.toLong + 5000)
            //更新 违法状态
            violationCarInfoState.update(value)
          }
        }

        //处理 出警信息，value : 当前这条数据，ctx:Flink 上下文，out:回收数据对象
        override def processElement2(value: PoliceInfo, ctx: KeyedCoProcessFunction[String, ViolationCarInfo, PoliceInfo, String]#Context, out: Collector[String]): Unit = {
          val info: ViolationCarInfo = violationCarInfoState.value()
          if (info != null) {
            //有违法信息
            out.collect(s"当前车辆${value.car}已经出警，出警信息：${value}")
            //取消定时器
            ctx.timerService().deleteEventTimeTimer(info.createTime.toLong + 5000)
            //清空违法信息状态
            violationCarInfoState.clear()
          } else {
            //没有违法信息,设置定时器，当前出警时间+5s后触发
            ctx.timerService().registerEventTimeTimer(value.policeTime + 5000)
            //更新出警状态
            policeInfoState.update(value)
          }
        }

        //定时器触发
        override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, ViolationCarInfo, PoliceInfo, String]#OnTimerContext, out: Collector[String]): Unit = {
          val state1: ViolationCarInfo = violationCarInfoState.value()
          val state2: PoliceInfo = policeInfoState.value()

          //有违法记录，没有出警记录
          if (state1 != null) {
            out.collect(s"车辆${state1.car}有违法记录，在5s内没有对应的出警记录")

          }
          //有出警记录，没有违法记录
          if (state2 != null) {
            out.collect(s"车辆${state2.car}有出警记录，在5s内没有对应的违法记录")
          }
          //清空状态
          policeInfoState.clear()
          violationCarInfoState.clear()

        }
      })
    resultDs.print()

    env.execute()




  }

}
