package cn.northpark.flink.scala.transformApp.util

//车辆监控实体
case class MonitorCarBean(areaId: String, roadId: String, monitorId: String, cameraId: String, actionTime: Long, car: String, speed: Double)


//最通畅的top5 通道实体
case class Top5MonitorBean(windowStartTime: String, windowEndTime: String, monitorId: String, hightSpeedCarCount: Long, middleSpeedCount: Long, normalSpeedCarCount: Long, lowSpeedCarCount: Long)

//通道通过车辆数的统计实体
case class MonitorSpeedClsNumsBean(xhightSpeedCarCount: Long, xmiddleSpeedCount: Long, xnormalSpeedCarCount: Long, xlowSpeedCarCount: Long) extends Ordered[MonitorSpeedClsNumsBean] {
  var hightSpeedCarCount = xhightSpeedCarCount
  var middleSpeedCount = xmiddleSpeedCount
  var normalSpeedCarCount = xnormalSpeedCarCount
  var lowSpeedCarCount = xlowSpeedCarCount

  override def compare(that: MonitorSpeedClsNumsBean): Int = {
    if (this.hightSpeedCarCount != that.hightSpeedCarCount) {
      (this.hightSpeedCarCount - that.hightSpeedCarCount).toInt
    } else if (this.middleSpeedCount != that.middleSpeedCount) {
      (this.middleSpeedCount - that.middleSpeedCount).toInt
    } else if (this.normalSpeedCarCount != that.normalSpeedCarCount) {
      (this.normalSpeedCarCount - that.normalSpeedCarCount).toInt
    } else {
      (this.lowSpeedCarCount - that.lowSpeedCarCount).toInt
    }
  }
}


object TrafficEntity {

}
