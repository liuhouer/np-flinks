package cn.northpark.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 滚动窗口，不分组划分窗口，将整体当成一个组,5秒一个窗口
 */
public class TumblingWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 4000);

        SingleOutputStreamOperator<Integer> map = source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });
        //不分组，将整体当成一个组
        AllWindowedStream<Integer, TimeWindow> windowAll = map.timeWindowAll(Time.seconds(5));


        SingleOutputStreamOperator<Integer> summed = windowAll.sum(0);

        summed.print();

        env.execute("TumblingWindowAll");


    }
}
