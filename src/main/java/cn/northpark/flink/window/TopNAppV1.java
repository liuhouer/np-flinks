package cn.northpark.flink.window;

import cn.northpark.flink.bean.Access;
import cn.northpark.flink.bean.EventCatagoryProductCount;
import cn.northpark.flink.window.udf.TopNAggregateFunction;
import cn.northpark.flink.window.udf.TopNWindowFunction;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 利用flink DataStream API来实现每N段时间内的统计，这里有区别于flinksql进行统计,稍微复杂一些
 */
public class TopNAppV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        SingleOutputStreamOperator<Access> cleanStream = env.readTextFile("data/access.json")
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        // json ==> 自定义对象
                        try {
                            return JSON.parseObject(value, Access.class);
                        } catch (Exception e) {
                            e.printStackTrace();
                            // TODO... 把这些异常的数据记录到某个地方去
                            return null;
                        }
                    }
                }).filter(x -> x != null)
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.seconds(20)) {
                            @Override
                            public long extractTimestamp(Access element) {
                                return element.time;
                            }
                        }
                ).filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return !"startup".equals(value.event);
                    }
                });

        //按照某些字段keyby
        WindowedStream<Access, Tuple3<String, String, String>, TimeWindow> windowStream = cleanStream.keyBy(new KeySelector<Access, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Access value) throws Exception {
                return Tuple3.of(value.event, value.product.category, value.product.name);
            }
        }).window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)));

        // 作用上WindowFunction

        //窗口聚合函数，先累加，再进行筛选计算topN
        SingleOutputStreamOperator<EventCatagoryProductCount> aggStream = windowStream.aggregate(new TopNAggregateFunction(), new TopNWindowFunction());

        aggStream.keyBy(new KeySelector<EventCatagoryProductCount, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> getKey(EventCatagoryProductCount value) throws Exception {
                return Tuple4.of(value.event, value.catagory, value.start, value.end);
            }
        }).process(new KeyedProcessFunction<Tuple4<String,String,Long,Long>, EventCatagoryProductCount, List<EventCatagoryProductCount>>() {

            private transient ListState<EventCatagoryProductCount> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<EventCatagoryProductCount>("cnt-state", EventCatagoryProductCount.class));
            }

            @Override
            public void processElement(EventCatagoryProductCount value, Context ctx, Collector<List<EventCatagoryProductCount>> out) throws Exception {
                listState.add(value);

                // 注册一个定时器
                ctx.timerService().registerEventTimeTimer(value.end + 1);
            }


            // 在这里完成TopN操作
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<EventCatagoryProductCount>> out) throws Exception {
                ArrayList<EventCatagoryProductCount> list = Lists.newArrayList(listState.get());

                list.sort((x,y) -> Long.compare(y.count, x.count));

                ArrayList<EventCatagoryProductCount> sorted = new ArrayList<>();

                for (int i = 0; i < Math.min(3, list.size()); i++) {
                    EventCatagoryProductCount bean = list.get(i);
                    sorted.add(bean);
                }

                out.collect(sorted);
            }
        }).print().setParallelism(1);

        env.execute();
    }

}
