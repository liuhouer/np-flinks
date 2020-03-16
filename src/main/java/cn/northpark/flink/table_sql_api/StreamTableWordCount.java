package cn.northpark.flink.table_sql_api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamTableWordCount {
    public static void main(String[] args) throws Exception {

        //实时dataStream api
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //实时Table执行上下文
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //word count java scala
        DataStreamSource<String> lines = env.socketTextStream("localhost", 4000);

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                Arrays.stream(value.split(" ")).forEach(out::collect);
            }
        });

        //将dataStream注册成表
        Table table = tableEnv.fromDataStream(words, "word");

        System.out.printf("schema---", table.getSchema());

        Table table2 = table.groupBy("word").select("word, count(1) as counts");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table2, Row.class);

//        tuple2DataStream.print();


        tuple2DataStream.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Row> value) throws Exception {

                return value.f0;
            }
        }).print();

        env.execute("StreamTableWordCount");
    }
}
