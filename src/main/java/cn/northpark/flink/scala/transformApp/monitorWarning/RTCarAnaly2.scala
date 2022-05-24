package cn.northpark.flink.scala.transformApp.monitorWarning

import java.util.Properties

import cn.northpark.flink.scala.transformApp.util._
import com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * 使用布隆过滤器来去重每个区域的车辆信息
  */
object RTCarAnaly2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //kafka 配置

    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","group112601")

    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("monitortopic1125",new SimpleStringSchema(),props).setStartFromEarliest())

    val carDS: DataStream[MonitorCarInfo] = ds.map(line => {
      val arr: Array[String] = line.split("\t")
      MonitorCarInfo(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5), arr(6).toDouble)
    })

    //创建 布隆过滤器对象
    val bfMap = scala.collection.mutable.Map[String,BloomFilter[CharSequence]]()

    //carDS 使用布隆过滤器去重每个区域中的车辆信息
    carDS.keyBy(_.areaId).timeWindow(Time.minutes(1))
      .aggregate(new AggregateFunction[MonitorCarInfo,Long,Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: MonitorCarInfo, accumulator: Long): Long = {
          val car: String = value.car
          val areaId: String = value.areaId
          //判断当前这辆车有没有在对应的布隆过滤器中存储过
          //如果存储过，当前这辆车不计数，如果不存在，那么计数，并且将这辆车信息存储到 布隆过滤器中
          if(bfMap.contains(areaId)){
            //map中有当前区域对应的 布隆过滤器
            val bf: BloomFilter[CharSequence] = bfMap.get(areaId).get
            if(bf.mightContain(car)){
              //包含车辆
              accumulator
            }else{
              //不包含车辆
              bf.put(car)
              accumulator + 1L
            }
          }else{
            //当前map 中没有这个区域对应的布隆过滤器
            val bf: BloomFilter[CharSequence] = BloomFilter.create(Funnels.stringFunnel(),20000)
            bf.put(car)
            bfMap.put(areaId,bf)
            accumulator + 1L
          }
        }

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a +b
      },
        new WindowFunction[Long,String,String,TimeWindow] {
          override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[String]): Unit = {
            out.collect(s"时间窗口起始时间：${window.getStart} - ${window.getEnd},当前区域:${key},车辆总数：${input.last}")
          }
        }
      ).print()

    env.execute()

  }

}
