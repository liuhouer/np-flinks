package cn.northpark.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * eventTime 滚动窗口
 * 如果使用的是并行的source，例如kafkaSource,创建kafka的Topic时有多个分区
 * 每一个Source的分区都要满足触发的条件，整个窗口才会被触发
 *
 * 带 watermark水位线
 *
 * @author bruce
 */
public class EventTimeTumblingWindowWithWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置使用EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> source = env.socketTextStream("localhost", 4000)

                //提取时间字段
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(String line) {
                        String[] fields = line.split(" ");
                        return Long.parseLong(fields[0]);
                    }
                });


        //1000 spark 1
        //1999 spark 2
        //4999 java 3
        SingleOutputStreamOperator<Tuple2<String,Integer>> map = source.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] lines = value.split(" ");
                return Tuple2.of(lines[1],Integer.parseInt(lines[2]));
            }
        });

        //先分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = map.keyBy(0);

        //按照分组后分窗口
//        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.timeWindow(Time.seconds(5));
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.window(TumblingEventTimeWindows.of(Time.seconds(5)));


        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = window.sum(1);

        summed.print();

        env.execute("EventTimeTumblingWindow");


    }
}
