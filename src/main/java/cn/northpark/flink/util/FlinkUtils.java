package cn.northpark.flink.util;

import com.twitter.chill.thrift.TBaseSerializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtils {

    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * 从kafka读取数据
     *
     * @return
     */
    public static <T> DataStream<T> createKafkaStream(ParameterTool parameters, Class<? extends DeserializationSchema> clazz) throws  Exception{

        env.getConfig().setGlobalJobParameters(parameters);

        env.getConfig().addDefaultKryoSerializer(Connection.class, TBaseSerializer.class);

        //开启checkpoint，并且开启重启策略
        env.enableCheckpointing(parameters.getLong("checkpoint.interval",5000L), CheckpointingMode.EXACTLY_ONCE);

        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);

        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //取消任务以后不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        //指定Ka fka的Broker地址
        props.setProperty("bootstrap.servers", parameters.getRequired("bootstrap.servers"));
        //指定组ID
        props.setProperty("group.id", parameters.get("group.id"));
        //如果没有记录偏移量，第一次从最开始消费
//        props.setProperty("auto.offset.reset", parameters.get("auto.offset.reset","earliest"));
        //kafka的消费者不自动提交偏移量
        props.setProperty("enable.auto.commit", parameters.get("enable.auto.commit","false"));

        List<String> topics = Arrays.asList(parameters.get("topics").split(","));
        //KafkaSource
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<T>(
                topics,
                clazz.newInstance(),
                props);

        //Flink从kafka读数据，开启checkpoint，会把偏移量写到statebackend，
        //默认也会写到kakfa一个特殊的topic中，可以关闭这个选项。
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        return env.addSource(kafkaConsumer);
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }
}
