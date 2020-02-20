package cn.northpark.flink;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author zhangyang
 *	使用Kafka作为数据源读取数据 exactly once
 */
public class KafkaSourceV2 {

    public static void main(String[] args) throws Exception {

        //1.环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.enableCheckpointing(5000);

        env.setStateBackend(new FsStateBackend("file:///Users/bruce/Documents/workspace/np-flink/backEnd"));

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));

        //精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties props = new Properties();

        //指定Kafka的Broker地址
        props.setProperty( "bootstrap.servers", "localhost:9092");
        //指定组ID
        props.setProperty("group.id", "bruce");
        //如果没有记录偏移量，第一次从最开始消费
        props.setProperty("auto.offset.reset", "earliest") ;
        //kafka的消费者不自动提交偏移量
        props.setProperty("enable.auto.commit", "false");

        //2.read
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink000", new SimpleStringSchema(), props);


        DataStream<String> lines = env.addSource(kafkaSource);

        //3.sink/transform
        lines.print();

        //4.execute
        env.execute("KafkaSourceV2");
    }

}
