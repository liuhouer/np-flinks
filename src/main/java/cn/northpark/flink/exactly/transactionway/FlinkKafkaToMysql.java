package cn.northpark.flink.exactly.transactionway;

import cn.northpark.flink.exactly.overrideway.MyRedisSink;
import cn.northpark.flink.util.FlinkUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

/***
 * Flink从kafka读取数据写入mysql 并且实现exactly once
 * @author bruce
 */
public class FlinkKafkaToMysql {

    public static void main(String[] args) throws  Exception{

//        ParameterTool parameters = ParameterTool.fromArgs(args);

        ParameterTool parameters  = ParameterTool.fromPropertiesFile("/Users/bruce/Documents/workspace/np-flink/src/main/resources/config.properties");

        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);


        // 拆词
        SingleOutputStreamOperator<String> words = kafkaStream.flatMap(new FlatMapFunction<String, String>() {
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

        sumed.map(new MapFunction<Tuple2<String, Integer>, Tuple3<String, String,String>>() {
            @Override
            public Tuple3<String, String, String> map(Tuple2<String, Integer> value) throws Exception {
                return Tuple3.of("NP-wordcount-sink-mysql",value.f0,value.f1.toString() );
            }
        }).addSink(new MyRedisSink());

        FlinkUtils.getEnv().execute("FlinkKafkaToMysql");

    }
}
