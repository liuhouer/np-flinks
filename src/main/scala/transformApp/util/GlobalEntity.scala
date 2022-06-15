package cn.northpark.flink.scala.transformApp.util

//定义车辆监控基本信息
case class MonitorCarInfo(areaId:String,roadId:String,monitorId:String,cameraId:String,actionTime:Long,car:String,speed:Double)
//定义车辆监控基本信息 + 限速信息
case class NewMonitorCarInfo(areaId:String,roadId:String,monitorId:String,cameraId:String,actionTime:Long,car:String,speed:Double,speedLimit:Double)

//定义车辆限速的信息
case class MonitorLimitSpeedInfo(areaId:String,roadId:String,monitorId:String,limitSpeed:Double)

//定义超速车辆的信息
case class OverSpeedCarInfo(car:String,monitorId:String,roadId:String,realSpeed:Double,limitSpeed:Double,actionTime:Long)

//定义卡扣平均速度信息
case class MonitorAvgSpeedInfo(windowStartTime:String,windowEndTime:String,monitorId:String,avgSpeed:Double,carCount:Long)

//定义最通畅的top5 卡扣信息
case class Top5MonitorInfo(windowStartTime:String,windowEndTime:String,monitorId:String,hightSpeedCarCount:Long,middleSpeedCount:Long,normalSpeedCarCount:Long,lowSpeedCarCount:Long)

//定义卡扣通过车辆数的统计对象
case class MonitorSpeedClsCount(xhightSpeedCarCount:Long,xmiddleSpeedCount:Long,xnormalSpeedCarCount:Long,xlowSpeedCarCount:Long) extends Ordered[MonitorSpeedClsCount]{
  var hightSpeedCarCount = xhightSpeedCarCount
  var middleSpeedCount = xmiddleSpeedCount
  var normalSpeedCarCount = xnormalSpeedCarCount
  var lowSpeedCarCount = xlowSpeedCarCount

  override def compare(that: MonitorSpeedClsCount): Int = {
    //先比较 高速
    if(this.hightSpeedCarCount != that.hightSpeedCarCount){
      (this.hightSpeedCarCount - that.hightSpeedCarCount).toInt
    }else if(this.middleSpeedCount != that.middleSpeedCount){
      (this.middleSpeedCount - that.middleSpeedCount).toInt
    }else if(this.normalSpeedCarCount != that.normalSpeedCarCount){
      (this.normalSpeedCarCount - that.normalSpeedCarCount).toInt
    }else{
      (this.lowSpeedCarCount - that.lowSpeedCarCount).toInt
    }
  }
}

//定义违法车辆信息
case class ViolationCarInfo(car:String,violation:String,createTime:String,detail:String)

//定义出警信息
case class PoliceInfo(policeId:String,car:String,policeTime:Long,policeState:String)



object GlobalEntity {

}
