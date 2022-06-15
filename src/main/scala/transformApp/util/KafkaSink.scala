package cn.northpark.flink.scala.transformApp.util

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

class KafkaSink[T](cls:String)(topic: String) extends RichSinkFunction[T] {
  var props :Properties = _
  //创建Kafka Producer
  var producer: KafkaProducer[String, String] = _
  //当初始化 RichSinnkFunction时，只会调用一次
  override def open(parameters: Configuration): Unit = {
    //创建kafka配置
    props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)

    producer = new KafkaProducer[String,String](props)
  }

  //来一条数据，处理一次
  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    //统计最通畅的top5通道
    val info: Top5MonitorBean = value.asInstanceOf[Top5MonitorBean]

    //向kafka中写入

    producer.send( new ProducerRecord[String,String](topic,info.toString))

  }

  override def close(): Unit = {
    producer.close()
  }
}
