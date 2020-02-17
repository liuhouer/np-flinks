package cn.northpark.flink.project.syncIO;

import cn.northpark.flink.project.syncIO.function.AsyncMysqlToActivityBeanFunciton;
import cn.northpark.flink.util.FlinkUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.concurrent.TimeUnit;

/**
 *
 * Kafka-console-producer --broker-list localhost:9092 --topic flink000
 * >A1
 * ----------------
 * 1> 新人礼包
 *
 * 通过连接池异步IO调用数据库关联查询的DEMO
 */
public class AsyncMysqlApplication {
    public static void main(String[] args) throws Exception {

        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args,new SimpleStringSchema());

        //调用异步IO的transform
        SingleOutputStreamOperator<String> strs = AsyncDataStream.unorderedWait(lines, new AsyncMysqlToActivityBeanFunciton(), 0, TimeUnit.SECONDS);

        strs.print();

        FlinkUtilsV1.getEnv().execute("AsyncHandleActivityLocationsApplication");

    }

}
