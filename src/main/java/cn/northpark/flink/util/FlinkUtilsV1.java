package cn.northpark.flink.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkUtilsV1 {

    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * 从kafka读取数据
     *
     * @param args
     * @param simpleStringSchema
     * @return
     */
    public static DataStream<String> createKafkaStream(String[] args, SimpleStringSchema simpleStringSchema) {

        String topic = args[0];
        String groupId = args[1];
        String brokerList = args[2];
        Properties props = new Properties();
        //指定Ka fka的Broker地址
        props.setProperty("bootstrap.servers", brokerList);
        //指定组ID
        props.setProperty("group.id", groupId);
        //如果没有记录偏移量，第一次从最开始消费
        props.setProperty("auto.offset.reset", "earliest");
        //kafka的消费者不自动提交偏移量
        //props . setProperty("enable.auto. commit", "false");
        //KafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                props);


        return env.addSource(kafkaSource);
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }
}
