package cn.northpark.flink.keyWordApp;


import cn.northpark.flink.util.KafkaString;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class keyWord {
    public static void main(String[] args) throws Exception {

        try {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
            String keywordTopic = "flink_key1";//接收关键词
            String keywordLimitTopic = "flink_sent1";//将flink统计好的关键词发送到kafka
//            String localServers = "kafka连接地址";

            Properties props = new Properties();
            props.put("bootstrap.servers","PLAINTEXT://node1:9092,PLAINTEXT://node2:9092,PLAINTEXT://node3:9092");
            //props.put("zookeeper.connect", parameters.getRequired("zookeeper.connect"));
            props.put("group.id", "bruce");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.offset.reset", "latest");


            FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(keywordTopic, new SimpleStringSchema(), props);
            DataStreamSource<String> stringDataStreamSource = env.addSource(kafkaConsumer);
            stringDataStreamSource.print();
            SingleOutputStreamOperator<Tuple2<String, Integer>> productLimitCount = stringDataStreamSource.flatMap(new SendMesageBusinessDispose())
                    .keyBy(0)
                    .timeWindow(Time.seconds(5))
                    .sum(1)
                    .setParallelism(2);

//            Properties localProp = new Properties();
//            localProp.setProperty("bootstrap.servers", localServers);
            productLimitCount.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
                private transient  Properties properties ;
                private static final String SIGN = "&&*$";
                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    properties = KafkaString.buildBasicKafkaProperty();
                }

                @Override
                public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {

                    String f0 = value.f0;
                    Integer f1 = value.f1;
                    KafkaString.sendKafkaString(properties,keywordLimitTopic, f0+SIGN+f1);
                }
            });

            env.execute("keyWordLimit");
        } catch (Exception e) {
            System.out.println("error:{}"+e);
        }
    }
}

class SendMesageBusinessDispose implements FlatMapFunction<String, Tuple2<String, Integer>> {


    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] values = line.split(",");
        if (values.length > 1) {
            out.collect(new Tuple2<String, Integer>("keyWord", 1));
        }
    }
}


class MySerializationSchema implements SerializationSchema<Tuple2<String, Integer>> {
    private static final String SIGN = "&&*$";
    @Override
    public void open(InitializationContext context) throws Exception {

    }

    @Override
    public byte[] serialize(Tuple2<String, Integer> stringIntegerTuple2) {
        String f0 = stringIntegerTuple2.f0;
        Integer f1 = stringIntegerTuple2.f1;
        return (f0+SIGN+f1).getBytes();
    }
}
