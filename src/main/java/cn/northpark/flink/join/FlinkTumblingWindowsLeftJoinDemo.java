package cn.northpark.flink.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class FlinkTumblingWindowsLeftJoinDemo {
    public static void main(String[] args) throws Exception {
        int windowSize = 10;
        long delay = 5100L;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 设置数据源
        DataStream<Tuple3<String, String, Long>> leftSource = env.addSource(new StreamDataSourceA()).name("Demo Source");
        DataStream<Tuple3<String, String, Long>> rightSource = env.addSource(new StreamDataSourceB()).name("Demo Source");

        // 设置水位线
        DataStream<Tuple3<String, String, Long>> leftStream = leftSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                }
        );

        DataStream<Tuple3<String, String, Long>> rigjhtStream = rightSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                }
        );

        // join 操作
        leftStream.coGroup(rigjhtStream)
                .where(new LeftSelectKey()).equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new LeftJoin())
                .print();

        env.execute("TimeWindowDemo");
    }

    public static class LeftJoin implements CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>> {
        @Override
        public void coGroup(Iterable<Tuple3<String, String, Long>> leftElements, Iterable<Tuple3<String, String, Long>> rightElements, Collector<Tuple5<String, String, String, Long, Long>> out) {

            for (Tuple3<String, String, Long> leftElem : leftElements) {
                boolean hadElements = false;
                for (Tuple3<String, String, Long> rightElem : rightElements) {
                    out.collect(new Tuple5<>(leftElem.f0, leftElem.f1, rightElem.f1, leftElem.f2, rightElem.f2));
                    hadElements = true;
                }
                if (!hadElements) {
                    out.collect(new Tuple5<>(leftElem.f0, leftElem.f1, "null", leftElem.f2, -1L));
                }
            }
        }
    }

    public static class LeftSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> w) {
            return w.f0;
        }
    }

    public static class RightSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> w) {
            return w.f0;
        }
    }

    // left join
    // coGroup算子
    //coGroup算子也是基于window窗口机制，不过coGroup算子比Join算子更加灵活，可以按照用户指定的逻辑匹配左流或右流数据并输出。
    //
    //换句话说，我们通过自己指定双流的输出来达到left join和right join的目的。
    //
    //现在来看看在相同场景下coGroup算子是如何实现left join:
    //
    //#这里看看java算子的写法
    //orderDetailStream
    //  .coGroup(orderStream)
    //  .where(r -> r.getOrderId())
    //  .equalTo(r -> r.getOrderId())
    //  .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
    //  .apply(new CoGroupFunction<OrderDetail, Order, Tuple2<String, Long>>() {
    //    @Override
    //    public void coGroup(Iterable<OrderDetail> orderDetailRecords, Iterable<Order> orderRecords, Collector<Tuple2<String, Long>> collector)  {
    //      for (OrderDetail orderDetaill : orderDetailRecords) {
    //        boolean flag = false;
    //        for (Order orderRecord : orderRecords) {
    //          // 右流中有对应的记录
    //          collector.collect(new Tuple2<>(orderDetailRecords.getGoods_name(), orderDetailRecords.getGoods_price()));
    //          flag = true;
    //        }
    //        if (!flag) {
    //          // 右流中没有对应的记录
    //          collector.collect(new Tuple2<>(orderDetailRecords.getGoods_name(), null));
    //        }
    //      }
    //    }
    //  })
    //  .print();
}