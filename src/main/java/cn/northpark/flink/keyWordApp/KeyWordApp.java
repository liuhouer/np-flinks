package cn.northpark.flink.keyWordApp;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author bruce
 * 自定义sink kafka的格式
 *
 */
public class KeyWordApp {
    public static void main(String[] args) throws Exception {

        try {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
            String keywordTopic = "flink_key1";//接收关键词
            String keywordLimitTopic = "flink_sent1";//将flink统计好的关键词发送到kafka

            Properties props = new Properties();
            props.put("bootstrap.servers","PLAINTEXT://node1:9092,PLAINTEXT://node2:9092,PLAINTEXT://node3:9092");
            props.put("group.id", "bruce");
            props.put("auto.offset.reset", "latest");


            FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(keywordTopic, new SimpleStringSchema(), props);
            DataStreamSource<String> stringDataStreamSource = env.addSource(kafkaConsumer);
            SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource
                    .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                        @Override
                        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                            String[] tokens = value.toLowerCase().split(",");
                            for(String token : tokens) {
                                if(token.length() > 0) {
                                    out.collect(new Tuple2<String,Integer>(token,1));
                                }
                            }
                        }
                    });

            SingleOutputStreamOperator<Tuple2<String, Integer>> productLimitCount = tuple2SingleOutputStreamOperator
                    .keyBy(0)
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                    .sum(1);

            //消费者
            FlinkKafkaProducer<Tuple2<String, Integer>> producer = new FlinkKafkaProducer<>(keywordLimitTopic,
                    new MySerializationSchema(keywordLimitTopic),
                    props,
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

            productLimitCount.addSink(producer);

            env.execute("KeyWordApp");
        } catch (Exception e) {
            System.out.println("error:{}"+e);
        }
    }
}


class MySerializationSchema implements KafkaSerializationSchema<Tuple2<String, Integer>> {
    private static final String SIGN = "&&*$";

    private String topic;
    private ObjectMapper mapper;

    public MySerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> stringIntegerTuple2, @Nullable Long timestamp) {
        byte[] b = null;
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            b= mapper.writeValueAsBytes(stringIntegerTuple2.f0+SIGN+stringIntegerTuple2.f1);
        } catch (JsonProcessingException e) {
            // 注意，在生产环境这是个非常危险的操作，
            // 过多的错误打印会严重影响系统性能，请根据生产环境情况做调整
            e.printStackTrace();
        }
        return new ProducerRecord<byte[], byte[]>(topic, b);
    }
}
