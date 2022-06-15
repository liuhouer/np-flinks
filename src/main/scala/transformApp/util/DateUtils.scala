package cn.northpark.flink.scala.transformApp.util

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

object DateUtils {
  //获取当前天的日期
  def getCurrentDate() = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(new Date())
  }
  //随机获取一个小时
  def getRandomHour() = {
    val random = new Random()
    random.nextInt(24).formatted("%02d")
  }
  //随机获取分钟或者秒
  def getRandomMinutesOrSeconds() = {
    val random = new Random()
    random.nextInt(60).formatted("%02d")
  }

  //根据时间戳转换成 yyyy-mm-dd HH:mm:ss 数据格式
  def timestampToDataStr(timeStamp:Long): String = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.format(new Date(timeStamp))
  }

}
