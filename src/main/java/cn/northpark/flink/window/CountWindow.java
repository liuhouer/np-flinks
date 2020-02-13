package cn.northpark.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 先分组，每个组达到一定数目才会被触发窗口
 * @author bruce
 */
public class CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 4000);

        //spark 1
        //spark 2
        //java 3
        SingleOutputStreamOperator<Tuple2<String,Integer>> map = source.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] lines = value.split(" ");
                return Tuple2.of(lines[0],Integer.parseInt(lines[1]));
            }
        });

        //先分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = map.keyBy(0);

        //按照分组后分窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> window = keyed.countWindow(5);


        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = window.sum(1);

        summed.print();

        env.execute("CountWindow");


    }
}
