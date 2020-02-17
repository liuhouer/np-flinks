package cn.northpark.flink.project.syncIO;

import cn.northpark.flink.project.ActivityBean;
import cn.northpark.flink.project.syncIO.function.AsyncRestfulToActivityBeanFunciton;
import cn.northpark.flink.project.syncIO.function.NP_MySqlSinkFunction;
import cn.northpark.flink.util.FlinkUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.concurrent.TimeUnit;

/***
 * 统计event，省份等纬度的数目，把结果写到t_activity_counts表
 * 写入mysql数据库DEMO
 */
public class SinkToMysqlApplication {
    public static void main(String[] args) throws Exception {

        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args,new SimpleStringSchema());

        //调用异步IO的transform
        SingleOutputStreamOperator<ActivityBean> beans = AsyncDataStream.unorderedWait(lines, new AsyncRestfulToActivityBeanFunciton(), 0, TimeUnit.SECONDS, 10);

        SingleOutputStreamOperator<ActivityBean> summed = beans.keyBy("eventType").sum("counts");

        SingleOutputStreamOperator<ActivityBean> summed2 = beans.keyBy("eventType","province").sum("counts");

        DataStreamSink<ActivityBean> addSink = summed.addSink(new NP_MySqlSinkFunction());

//        DataStreamSink<ActivityBean> addSink2 = summed2.addSink(new NP_MySqlSink());

        FlinkUtilsV1.getEnv().execute("SinkToMysqlApplication");

    }
}
