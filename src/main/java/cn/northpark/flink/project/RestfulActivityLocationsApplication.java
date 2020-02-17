package cn.northpark.flink.project;

import cn.northpark.flink.project.function.RestfulToActivityBeanFunciton;
import cn.northpark.flink.util.FlinkUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 *
 * 需求2:查询高德地图API,关联地理位置信息
 *
 * 给定的数据:
 * u001,A1,2019-09-02 10:10:11,1,115.908923,39.267291
 * u002,A1,2019-09-02 10:11:11,1,123.818517,41.312458
 * u003,A2,2019-09-02 10:13:11,1,121.26757,37.49794
 *
 * 希望的得到的数据
 * u001,A1,2019-09-02 10:10:11,1,北京市
 * u002,A1,2019-09-02 10:11:11,1,辽宁省
 *
 */
public class RestfulActivityLocationsApplication {

    public static void main(String[] args) throws Exception {

        DataStream<String>  lines = FlinkUtilsV1.createKafkaStream(args,new SimpleStringSchema());

        SingleOutputStreamOperator<ActivityBean> beans = lines.map(new RestfulToActivityBeanFunciton());

        beans.print();

        FlinkUtilsV1.getEnv().execute("HandleActivityLocationsApplication");

    }
}
