package cn.northpark.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhangyang
 * 应用state自己实现sum功能
 */
public class SumWithState {

	public static void main(String[] args) throws Exception {

        //1.环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        Properties props = new Properties();
//
//        //指定Kafka的Broker地址
//        props.setProperty( "bootstrap.servers", "localhost:9092");
//        //指定组ID
//        props.setProperty("group.id", "bruce");
//        //如果没有记录偏移量，第一次从最开始消费
//        props.setProperty("auto.offset.reset", "earliest") ;
//        //kafka的消费者不自动提交偏移量****
//        props.setProperty("enable.auto.commit", "false");
//
//        //2.read
//        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink000", new SimpleStringSchema(), props);

//        DataStream<String> lines = env.addSource(kafkaSource);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 4000);



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
                if(value.equals("jeyy")){
                    System.out.println(1/0);
                }
                return Tuple2.of(value, 1);
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = wordAndOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyBy.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private transient  ValueState<Tuple2<String, Integer>> state ;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<Tuple2<String, Integer>>("np-sum-state", Types.TUPLE(Types.STRING,Types.INT));
                state = getRuntimeContext().getState(descriptor);


            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                String word = value.f0;
                int count = value.f1;
                Tuple2<String, Integer> hisData = state.value();
                if(hisData!=null){
                    hisData.f1 +=count;
                    state.update(hisData);
                    return  hisData;
                }else{
                    state.update(value);
                    return  value;
                }
            }

        });

        //sink
        summed.print();

        //execute
        env.execute("SumWithState");
	}
}
