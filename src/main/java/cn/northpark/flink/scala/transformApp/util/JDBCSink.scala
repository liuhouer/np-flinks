package cn.northpark.flink.scala.transformApp.util

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class JDBCSink[T](cls:String) extends RichSinkFunction[T] {
  var conn: Connection = _
  var pst: PreparedStatement = _
  var stop = false
  //当初始化 RichSinnkFunction时，只会调用一次
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink", "root", "123456")
  }

  //来一条数据，处理一次
  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    if("Top5MonitorInfo".equals(cls)){
      //统计最通畅的top5通道
      val info: Top5MonitorBean = value.asInstanceOf[Top5MonitorBean]
      pst = conn.prepareStatement("insert into t_top5_monitor_info (start_time,end_time,monitor_id,hight_speed_carcount,middle_speed_carcount,normal_speed_carcount,low_speed_carcount) values(?,?,?,?,?,?,?)")
      pst.setString(1,info.windowStartTime)
      pst.setString(2,info.windowEndTime)
      pst.setString(3,info.monitorId)
      pst.setDouble(4,info.hightSpeedCarCount)
      pst.setDouble(5,info.middleSpeedCount)
      pst.setDouble(6,info.normalSpeedCarCount)
      pst.setDouble(7,info.lowSpeedCarCount)
      pst.executeUpdate()
    }

  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }
}
