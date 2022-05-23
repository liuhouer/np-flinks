package cn.northpark.flink.scala.transformApp.app

import java.util.Properties

import cn.northpark.flink.scala.transformApp.util._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * 数据格式预览
 * 区域id,道路id, 通道id,  摄像头id, 拍摄时间，     车辆信息， 车辆速度
 * 04     47	    1165	43238	    1606313977000	京D58951	11.6
 * 06     37	    7032	88654	    1606312710000	京D58951	66.25
 * 06     16	    5836	34988	    1606241154000	京D58951	40.56
 * 01     28	    8488	42469	    1606306949000	京D58951	42.03
 * 04     03	    4018	11193	    1606302645000	京D58951	49.06
 * 00     22	    7434	51957	    1606319884000	京D58951	26.69
 * 01     39	    5952	61613	    1606242062000	京D58951	65.08
 * 03     16	    1882	33415	    1606309815000	京D58951	128.73
 * 02     34	    9635	48741	    1606318904000	京D58951	90.27
 * 00     36	    0304	14633	    1606265350000	京D58951	74.75
 * 01     02	    1261	42126	    1606301535000	京D58951	15.29
 * 02     24	    1204	41915	    1606291261000	京D58951	28.06
 * 02     13	    9712	65130	    1606295428000	京D58951	72.81
 * 05     38	    3734	10102	    1606279310000	京D58951	3.8
 * 06     04	    6436	91270	    1606310077000	京D58951	29.66
 * 00     26	    1043	46829	    1606280080000	京D58951	53.55
 * 04     15	    5119	58666	    1606260136000	京D58951	3.25
 * 03     18	    5507	35247	    1606267850000	京D58951	70.52
  * 每隔1分钟统计最近5分钟 最通畅的topN通道信息
  */
object TopNMinCars {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    //设置并行度和事件时间
//    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取Kafka中 实时监控车辆数据
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","bruce")
    props.setProperty("auto.offset.reset","latest")

    //读取Kafka 中监控到实时的车辆信息
    val monitorInfosDs: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink_traffic4",new SimpleStringSchema(),props).setStartFromEarliest())
    //数据类型转换
    val transferDS: DataStream[MonitorCarBean] = monitorInfosDs.map(line => {
      val arr: Array[String] = line.split("\t")
      MonitorCarBean(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5), arr(6).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MonitorCarBean](Time.seconds(5)) {
      override def extractTimestamp(element: MonitorCarBean): Long = element.actionTime
    })

//    transferDS.print("接收到消息并转为实体：")

    //每隔2秒钟统计过去10秒钟，最通畅的top5通道信息
    val Top5MonitorBeanDS: DataStream[Top5MonitorBean] = transferDS.timeWindowAll(Time.seconds(60), Time.seconds(10))
      .process(new ProcessAllWindowFunction[MonitorCarBean, Top5MonitorBean, TimeWindow] {
        //context :Flink 上下文，elements : 窗口期内所有元素，out: 回收数据对象
        override def process(context: Context, elements: Iterable[MonitorCarBean], out: Collector[Top5MonitorBean]): Unit = {
          val map = scala.collection.mutable.Map[String, MonitorSpeedClsNumsBean]()
          val iter: Iterator[MonitorCarBean] = elements.iterator
          while (iter.hasNext) {
            val currentInfo: MonitorCarBean = iter.next()
            val areaId: String = currentInfo.areaId
            val roadId: String = currentInfo.roadId
            val monitorId: String = currentInfo.monitorId
            val speed: Double = currentInfo.speed
            val currentKey = areaId + "_" + roadId + "_" + monitorId
            //判断当前map中是否含有当前本条数据对应的 区域_道路_通道的信息
            if (map.contains(currentKey)) {
              //判断当前此条车辆速度位于哪个速度端，给map中当前key 对应的value MonitorSpeedClsNumsBean 对象对应的速度段加1
              if (speed >= 120) {
                map.get(currentKey).get.hightSpeedCarCount += 1
              } else if (speed >= 90) {
                map.get(currentKey).get.middleSpeedCount += 1
              } else if (speed >= 60) {
                map.get(currentKey).get.normalSpeedCarCount += 1
              } else {
                map.get(currentKey).get.lowSpeedCarCount += 1
              }
            } else {
              //不包含 当前key
              val mscc = MonitorSpeedClsNumsBean(0L, 0L, 0L, 0L)
              if (speed >= 120) {
                mscc.hightSpeedCarCount += 1
              } else if (speed >= 90) {
                mscc.middleSpeedCount += 1
              } else if (speed >= 60) {
                mscc.normalSpeedCarCount += 1
              } else {
                mscc.lowSpeedCarCount += 1
              }
              map.put(currentKey, mscc)
            }
          }

          val tuples: List[(String, MonitorSpeedClsNumsBean)] = map.toList.sortWith((tp1, tp2) => {
            tp1._2 > tp2._2
          }).take(5)
          for (elem <- tuples) {
            val windowStartTime: String = DateUtils.timestampToDataStr(context.window.getStart)
            val windowEndTime: String = DateUtils.timestampToDataStr(context.window.getEnd)
            out.collect(new Top5MonitorBean(windowStartTime, windowEndTime, elem._1, elem._2.hightSpeedCarCount, elem._2.middleSpeedCount, elem._2.normalSpeedCarCount, elem._2.lowSpeedCarCount))
          }

        }
      })

    //打印结果
    Top5MonitorBeanDS.print("计算TOP n");

    //通过jdbc 来sink到mysql
    Top5MonitorBeanDS.addSink(new JDBCSink[Top5MonitorBean]("Top5MonitorBean"))

    //sink 到 kafka新的topic
    Top5MonitorBeanDS.addSink(new KafkaSink[Top5MonitorBean]("Top5MonitorBean")("flink_sink_rs"))


    env.execute()

  }

}
