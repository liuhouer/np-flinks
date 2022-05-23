package cn.northpark.flink.scala.transformApp.util

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.commons.math3.random.{GaussianRandomGenerator, JDKRandomGenerator}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
  * 模拟生成数据
  *  1.将模拟的数据生成到文件中
  *  2.将模拟的数据生成到Kafka中
  *  区域id,道路id,卡扣id,摄像头id,拍摄时间，车辆信息，车辆速度
  */
object MakeData {
  def main(args: Array[String]): Unit = {
    //创建写入数据的文件
    val pw = new PrintWriter("C:\\Users\\Bruce\\Desktop\\3\\trafficdata")
    //创建kafka配置
    val props = new Properties()
    props.setProperty("bootstrap.servers","mynode1:9092,mynode2:9092,mynode3:9092")
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)

    //创建Kafka Producer
    val producer = new KafkaProducer[String,String](props)

    //模拟3000辆车 京Axxxxx
    val locations = Array[String]("京","津","冀","京","鲁","京","京","京","京","京")
    val random = new Random()
    val generator = new GaussianRandomGenerator(new JDKRandomGenerator())

    for(i <- 1 to 3000){
      //模拟车辆
      val car = locations(random.nextInt(10))+(65+random.nextInt(26)).toChar+random.nextInt(99999).formatted("%05d")
      //模拟每辆车通过的卡扣数 ，一辆车每天通过卡扣数可能是大部分都不超过100个卡扣
      val throuldMonitorCount = (generator.nextNormalizedDouble() * 100).abs.toInt
      for(j <- 0 until throuldMonitorCount){
        //通过的区域
        val areaId = random.nextInt(8).formatted("%02d")
        //通过的道路
        val roadId = random.nextInt(50).formatted("%02d")
        //通过的卡扣
        val monitorId = random.nextInt(9999).formatted("%04d")
        //通过的摄像头
        val cameraId = random.nextInt(99999).formatted("%05d")
        //摄像头拍摄时间,转换成时间戳
        val yyyyMMddHHmmss =DateUtils.getCurrentDate() + " "+DateUtils.getRandomHour()+":"+DateUtils.getRandomMinutesOrSeconds+":"+DateUtils.getRandomMinutesOrSeconds
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val actionTime = format.parse(yyyyMMddHHmmss).getTime

        //拍摄车辆速度 ,大部分车辆速度位于60左右
        val speed :Double = (generator.nextNormalizedDouble()*60).abs.formatted("%.2f").toDouble

        val info = s"${areaId}\t${roadId}\t${monitorId}\t${cameraId}\t${actionTime}\t${car}\t${speed}"
        println(info)
        //向文件中写入
        pw.println(info)
        //向kafka中写入
        producer.send( new ProducerRecord[String,String]("monitortopic1125",info))
      }
    }
    pw.close()
    producer.close()

  }

}
