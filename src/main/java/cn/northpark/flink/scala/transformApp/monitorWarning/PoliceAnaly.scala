package cn.northpark.flink.scala.transformApp.monitorWarning

import cn.northpark.flink.scala.transformApp.util._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  *  出警分析
  *     违法记录：socket- 9999
  *       京A89721,涉嫌套牌,1606312710000,涉嫌套牌
  *       京A89132,事故,16063127120000,事故
  *
  *     出警记录：socket- 8888
  *       pid1,京A89721,16063127120000，成功
  */
object PoliceAnaly {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //获取违法信息
    val violationInfoDS: DataStream[String] = env.socketTextStream("mynode5",9999)

    //获取出警信息
    val policeInfoDS: DataStream[String] = env.socketTextStream("mynode5",8888)

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

    //对以上违法车辆信息及出警信息进行管理
    violationTransferDS.keyBy(_.car).intervalJoin(policeTransferDS.keyBy(_.car))

    /**
      * 如果在违法数据流中有一条违法数据发生 时间为：京A11111,9:10:30
      * 可以与后面的出警数据流中 9:10:20-9:10:40 之内的数据进行按照车牌进行关联
      */
      .between(Time.seconds(-10),Time.seconds(10))
      //匹配上之后的数据，就会被process执行
      .process(new ProcessJoinFunction[ViolationCarInfo,PoliceInfo,String] {
        //left : 左侧的流-违法数据流一条数据，right:出警数据流中的一条数据，ctx:Flink上下文，out:回收数据对象
        override def processElement(left: ViolationCarInfo, right: PoliceInfo, ctx: ProcessJoinFunction[ViolationCarInfo, PoliceInfo, String]#Context, out: Collector[String]): Unit = {
          out.collect(s"车辆：${left.car},违法时间:${left.createTime},出警时间：${right.policeTime},出警号:${right.policeId},出警状态:${right.policeState}")
        }
      }).print()

    env.execute()

  }

}
