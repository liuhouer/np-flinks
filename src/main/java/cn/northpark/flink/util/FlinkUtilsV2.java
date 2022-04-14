package cn.northpark.flink.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author bruce
 * @date 2022年04月14日 10:45:37
 * 执行程序的参数:配置kafka配置文件的绝对路径
 */
public class FlinkUtilsV2 {
    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    public static <T> DataStream<T> createKafkaDataStream(ParameterTool parameters, Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        String topics = parameters.getRequired("kafka.topics");
        String groupId = parameters.getRequired("group.id");
        return createKafkaDataStream(parameters, topics, groupId, clazz);
    }



    public static <T> DataStream<T> createKafkaDataStream(ParameterTool parameters, String topics, Class<? extends DeserializationSchema<T>> clazz) throws Exception {

        String groupId = parameters.getRequired("group.id");
        return createKafkaDataStream(parameters, topics, groupId, clazz);
    }


    public static <T> DataStream<T> createKafkaDataStream(ParameterTool parameters, String topics, String groupId, Class<? extends DeserializationSchema<T>> clazz) throws Exception {

        //将ParameterTool的参数设置成全局的参数
        env.getConfig().setGlobalJobParameters(parameters);

        //开启checkpoint
        env.enableCheckpointing(parameters.getLong("checkpoint.interval", 10000L), CheckpointingMode.EXACTLY_ONCE);

        //重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(parameters.getInt("restart.times", 10), Time.seconds(5)));

        //设置statebackend
        String path = parameters.get("state.backend.path");
        if(path != null) {
            //最好的方式将setStateBackend配置到Flink的全局配置文件中flink-conf.yaml
            env.setStateBackend(new FsStateBackend(path));
        }

        //设置cancel任务不用删除checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);

        //String topics = parameters.getRequired("kafka.topics");

        List<String> topicList = Arrays.asList(topics.split(","));

        Properties properties = parameters.getProperties();

        properties.setProperty("group.id", groupId);

        //创建FlinkKafkaConsumer
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<T>(
                topicList,
                clazz.newInstance(),
                properties
        );

        return env.addSource(kafkaConsumer);
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }

}
