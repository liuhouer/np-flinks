package cn.northpark.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author zhangyang
 * 按照步骤来一步步拆分Task是如何划分的
 * wc统计的数据我们源自于socket
 */
public class OperatorStateAndKeyedState {

	public static void main(String[] args) throws Exception {

        //1.环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();

        //指定Kafka的Broker地址
        props.setProperty( "bootstrap.servers", "localhost:9092");
        //指定组ID
        props.setProperty("group.id", "bruce");
        //如果没有记录偏移量，第一次从最开始消费
        props.setProperty("auto.offset.reset", "earliest") ;
        //kafka的消费者不自动提交偏移量
        props.setProperty("enable. auto. commit", "false");

        //2.read
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink000", new SimpleStringSchema(), props);


        DataStream<String> lines = env.addSource(kafkaSource);

        // 拆词
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //把单词和1拼一块
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //分组、累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(0).sum(1);//.setParallelism(1);


        //sink
        sumed.print();

        //execute
        env.execute("OperatorStateAndKeyedState");
	}
}
