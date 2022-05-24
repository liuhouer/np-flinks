package cn.northpark.flink.scala.transformApp.monitorWarning

import java.util.Properties

import cn.northpark.flink.scala.transformApp.util._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * 实时车辆区域分布统计
  */
object RTCarAnaly {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //kafka 配置

    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","group112601xx")

    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("monitortopic1125",new SimpleStringSchema(),props).setStartFromEarliest())

    val carDS: DataStream[MonitorCarInfo] = ds.map(line => {
      val arr: Array[String] = line.split("\t")
      MonitorCarInfo(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5), arr(6).toDouble)
    })

    //每个一分钟统计每个区域中的车辆总数
    carDS.keyBy(_.areaId)
      .timeWindow(Time.minutes(1))
      .apply(new WindowFunction[MonitorCarInfo,String,String,TimeWindow] {
        //key : 当前区域，window:当前窗口对象，input : 当前窗口内的数据，out : 回收数据对象
        override def apply(key: String, window: TimeWindow, input: Iterable[MonitorCarInfo], out: Collector[String]): Unit = {
          val carSet = scala.collection.mutable.Set[String]()

          val iter: Iterator[MonitorCarInfo] = input.iterator
          while(iter.hasNext){
            val mci: MonitorCarInfo = iter.next()
            carSet.add(mci.car)
          }
          out.collect(s"窗口起始时间：${window.getStart} - ${window.getEnd},当前区域：${key} ,车辆总数为：${carSet.size}")
        }
      }).print()

    env.execute()

  }
}
