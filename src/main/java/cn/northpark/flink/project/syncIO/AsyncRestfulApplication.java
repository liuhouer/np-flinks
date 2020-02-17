package cn.northpark.flink.project.syncIO;

import cn.northpark.flink.project.ActivityBean;
import cn.northpark.flink.project.syncIO.function.AsyncRestfulToActivityBeanFunciton;
import cn.northpark.flink.util.FlinkUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.concurrent.TimeUnit;

/**
 *
 * Kafka-console-producer --broker-list localhost:9092 --topic flink000
 * >u002,A1,2019-09-02 10:11:11,1,123.818517,41.312458
 * ----------------
 * 1> ActivityBean{uid='u002', aid='A1', activityName='null', time='2019-09-02 10:11:11', eventType=1, province='辽宁省', longitude=null, latitude=null}
 *
 * 通过Http  异步IO  调用restful请求高德接口获取关联的地区的DEMO
 */
public class AsyncRestfulApplication {
    public static void main(String[] args) throws Exception {

        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args,new SimpleStringSchema());

        //调用异步IO的transform
        SingleOutputStreamOperator<ActivityBean> beans = AsyncDataStream.unorderedWait(lines, new AsyncRestfulToActivityBeanFunciton(), 0, TimeUnit.SECONDS, 10);

        beans.print();

        FlinkUtilsV1.getEnv().execute("AsyncHandleActivityLocationsApplication");

    }

}
