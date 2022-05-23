package cn.northpark.flink.scala.transformApp.app

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import cn.northpark.flink.scala.transformApp.util._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * 数据格式预览
 * 04	47	1165	43238	1606313977000	京D58951	11.6
 * 06	37	7032	88654	1606312710000	京D58951	66.25
 * 06	16	5836	34988	1606241154000	京D58951	40.56
 * 01	28	8488	42469	1606306949000	京D58951	42.03
 * 04	03	4018	11193	1606302645000	京D58951	49.06
 * 00	22	7434	51957	1606319884000	京D58951	26.69
 * 01	39	5952	61613	1606242062000	京D58951	65.08
 * 03	16	1882	33415	1606309815000	京D58951	128.73
 * 02	34	9635	48741	1606318904000	京D58951	90.27
 * 00	36	0304	14633	1606265350000	京D58951	74.75
 * 01	02	1261	42126	1606301535000	京D58951	15.29
 * 02	24	1204	41915	1606291261000	京D58951	28.06
 * 02	13	9712	65130	1606295428000	京D58951	72.81
 * 05	38	3734	10102	1606279310000	京D58951	3.8
 * 06	04	6436	91270	1606310077000	京D58951	29.66
 * 00	26	1043	46829	1606280080000	京D58951	53.55
 * 04	15	5119	58666	1606260136000	京D58951	3.25
 * 03	18	5507	35247	1606267850000	京D58951	70.52
 *
  *  每隔1分钟统计过去5分钟，每个区域每个道路每个通道的平均速度
  */
object MonitorAvgSpeedAnaly {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //设置并行度和事件时间
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取Kafka中 实时监控车辆数据
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","group112502")

    //读取Kafka 中监控到实时的车辆信息
    val monitorInfosDs: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("monitortopic1125",new SimpleStringSchema(),props).setStartFromEarliest())
    //数据类型转换
    val transferDS: DataStream[MonitorCarInfo] = monitorInfosDs.map(line => {
      val arr: Array[String] = line.split("\t")
      MonitorCarInfo(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5), arr(6).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MonitorCarInfo](Time.seconds(5)) {
      override def extractTimestamp(element: MonitorCarInfo): Long = element.actionTime
    })


    //设置窗口，每隔1分钟计算每个通道的平均速度,使用aggregate 增量计算+全量计算
    val monitorAvgSpeedDS: DataStream[MonitorAvgSpeedInfo] = transferDS.keyBy(mi => {
      mi.areaId + "_" + mi.roadId + "_" + mi.monitorId
    }).timeWindow(Time.minutes(5), Time.minutes(1))
      .aggregate(new AggregateFunction[MonitorCarInfo, (Long, Double), (Long, Double)] {
        override def createAccumulator(): (Long, Double) = (0L, 0L)

        override def add(value: MonitorCarInfo, accumulator: (Long, Double)): (Long, Double) = {
          (accumulator._1 + 1, accumulator._2 + value.speed)
        }

        override def getResult(accumulator: (Long, Double)): (Long, Double) = accumulator

        override def merge(a: (Long, Double), b: (Long, Double)): (Long, Double) = (a._1 + b._1, a._2 + b._2)
      },
        new WindowFunction[(Long, Double), MonitorAvgSpeedInfo, String, TimeWindow] {
          //key : 区域_道路_通道  window : 当前窗口，input ：输入的数据 ，out:回收数据对象
          override def apply(key: String, window: TimeWindow, input: Iterable[(Long, Double)], out: Collector[MonitorAvgSpeedInfo]): Unit = {
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val startTime: String = sdf.format(new Date(window.getStart))
            val endTime: String = sdf.format(new Date(window.getEnd))
            //计算当前通道的平均速度
            val last: (Long, Double) = input.last
            val avgSpeed: Double = (last._2 / last._1).formatted("%.2f").toDouble
            out.collect(new MonitorAvgSpeedInfo(startTime, endTime, key, avgSpeed, last._1))
          }
        })

    //保存到mysql 数据库中
    monitorAvgSpeedDS.addSink(new JDBCSink[MonitorAvgSpeedInfo]("MonitorAvgSpeedInfo"))

    env.execute()




  }

}
