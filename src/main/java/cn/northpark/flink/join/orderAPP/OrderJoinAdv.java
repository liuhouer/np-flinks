package cn.northpark.flink.join.orderAPP;

import cn.northpark.flink.util.FlinkUtilsV2;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;

/**
 * @author bruce
 * @date 2022年04月14日 10:49:45
 *
 * <pre>订单数据和迟到数据join的实现</pre>
 *
 * 分析
 * 要想统计，需要将两个表中的数据拉取到Flink做Join关联。
 * 即要在同一个窗口中关联，就要划分窗口。
 * 划分窗口后，数据就有可能迟到，要处理迟到的数据。
 * 业务系统中数据，基本都是存储在关系型数据库中，如Mysql
 * 通过canal这个工具，可以把mysql中的业务数据，导入到kafka中（canal伪装成Mysql的Salve）
 * Flink通过 KafkaSource 从kafka中拉取业务数据
 * 拉取到的数据，要做处理，并将主表、从表关联起来（join）
 * 按EventTime划分窗口，处理迟到的数据
 * 然后再做统计成交金额
 *
 * 技术点
 * canal的使用及原理
 * kafka的生产者、消费者、Topic
 * Flink的EventTime滚动窗口 ☆☆☆
 * Flink的双流的LeftJoin ☆☆☆☆
 * Flink的测流输出迟到数据 ☆☆☆☆☆
 * Mysql的连接及查询 ☆☆
 *
 */
public class OrderJoinAdv {
    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        //使用EventTime作为时间标准
        FlinkUtilsV2.getEnv().setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> orderMainLinesDataStream = FlinkUtilsV2.createKafkaDataStream(parameters, "ordermain", "g1", SimpleStringSchema.class);

        DataStream<String> orderDetailLinesDataStream = FlinkUtilsV2.createKafkaDataStream(parameters, "orderdetail", "g1", SimpleStringSchema.class);

        //对数据进行解析
        SingleOutputStreamOperator<OrderMain> orderMainDataStream = orderMainLinesDataStream.process(new ProcessFunction<String, OrderMain>() {

            @Override
            public void processElement(String line, Context ctx, Collector<OrderMain> out) throws Exception {
                //flatMap+filter
                try {
                    JSONObject jsonObject = JSON.parseObject(line);
                    String type = jsonObject.getString("type");
                    if (type.equals("INSERT") || type.equals("UPDATE")) {
                        JSONArray jsonArray = jsonObject.getJSONArray("data");
                        for (int i = 0; i < jsonArray.size(); i++) {
                            OrderMain orderMain = jsonArray.getObject(i, OrderMain.class);
                            orderMain.setType(type); //设置操作类型
                            out.collect(orderMain);
                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                    //记录错误的数据
                }
            }
        });

        //对数据进行解析
        SingleOutputStreamOperator<OrderDetail> orderDetailDataStream = orderDetailLinesDataStream.process(new ProcessFunction<String, OrderDetail>() {

            @Override
            public void processElement(String line, Context ctx, Collector<OrderDetail> out) throws Exception {
                //flatMap+filter
                try {
                    JSONObject jsonObject = JSON.parseObject(line);
                    String type = jsonObject.getString("type");
                    if (type.equals("INSERT") || type.equals("UPDATE")) {
                        JSONArray jsonArray = jsonObject.getJSONArray("data");
                        for (int i = 0; i < jsonArray.size(); i++) {
                            OrderDetail orderDetail = jsonArray.getObject(i, OrderDetail.class);
                            orderDetail.setType(type); //设置操作类型
                            out.collect(orderDetail);
                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                    //记录错误的数据
                }
            }
        });

        int delaySeconds = 2;
        int windowSize = 5;

        //提取EventTime生成WaterMark
        SingleOutputStreamOperator<OrderMain> orderMainStreamWithWaterMark = orderMainDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderMain>(Time.seconds(delaySeconds)) {
            @Override
            public long extractTimestamp(OrderMain element) {
                return element.getCreate_time().getTime();
            }
        });

        SingleOutputStreamOperator<OrderDetail> orderDetailStreamWithWaterMark = orderDetailDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderDetail>(Time.seconds(delaySeconds)) {
            @Override
            public long extractTimestamp(OrderDetail element) {
                return element.getCreate_time().getTime();
            }
        });

        //定义迟到侧流输出的Tag
        OutputTag<OrderDetail> lateTag = new OutputTag<OrderDetail>("late-date") {};

        //对左表进行单独划分窗口，窗口的长度与cogroup的窗口长度一样
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWindow = orderDetailStreamWithWaterMark.windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .sideOutputLateData(lateTag) //将迟到的数据打上Tag
                .apply(new AllWindowFunction<OrderDetail, OrderDetail, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<OrderDetail> values, Collector<OrderDetail> out) throws Exception {
                        for (OrderDetail value : values) {
                            //out.collect(value);
                        }
                    }
                });

        //获取迟到的数据
        DataStream<OrderDetail> lateOrderDetailStream = orderDetailWithWindow.getSideOutput(lateTag);

        //应为orderDetail表的数据迟到数据不是很多，没必要使用异步IO，直接使用RichMapFunction
        SingleOutputStreamOperator<Tuple2<OrderDetail, OrderMain>> lateOrderDetailAndOrderMain = lateOrderDetailStream.map(new RichMapFunction<OrderDetail, Tuple2<OrderDetail, OrderMain>>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                //创建书库的连接
            }

            @Override
            public Tuple2<OrderDetail, OrderMain> map(OrderDetail value) throws Exception {
                return null;
            }

            @Override
            public void close() throws Exception {
                //关闭数据库的连接
            }
        });


        //Left Out JOIN，并且将订单明细表作为左表
        DataStream<Tuple2<OrderDetail, OrderMain>> joined = orderDetailWithWindow.coGroup(orderMainStreamWithWaterMark)
                .where(new KeySelector<OrderDetail, Long>() {
                    @Override
                    public Long getKey(OrderDetail value) throws Exception {
                        return value.getOrder_id();
                    }
                })
                .equalTo(new KeySelector<OrderMain, Long>() {
                    @Override
                    public Long getKey(OrderMain value) throws Exception {
                        return value.getOid();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new CoGroupFunction<OrderDetail, OrderMain, Tuple2<OrderDetail, OrderMain>>() {

                    @Override
                    public void coGroup(Iterable<OrderDetail> first, Iterable<OrderMain> second, Collector<Tuple2<OrderDetail, OrderMain>> out) throws Exception {

                        for (OrderDetail orderDetail : first) {
                            boolean isJoined = false;
                            for (OrderMain orderMain : second) {
                                out.collect(Tuple2.of(orderDetail, orderMain));
                                isJoined = true;
                            }
                            if (!isJoined) {
                                out.collect(Tuple2.of(orderDetail, null));
                            }
                        }
                    }
                });

        //join后，有可orderMain没有join上
        SingleOutputStreamOperator<Tuple2<OrderDetail, OrderMain>> punctualOrderDetailAndOrderMain = joined.map(new RichMapFunction<Tuple2<OrderDetail, OrderMain>, Tuple2<OrderDetail, OrderMain>>() {

            private transient Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                //打开数据库连接
            }

            @Override
            public Tuple2<OrderDetail, OrderMain> map(Tuple2<OrderDetail, OrderMain> tp) throws Exception {
                //每个关联上订单主表的数据，就查询书库
                if (tp.f1 == null) {
                    OrderMain orderMain = queryOrderMainFromMySQL(tp.f0.getOrder_id(), connection);
                    tp.f1 = orderMain;
                }
                return tp;
            }

            @Override
            public void close() throws Exception {
                //关闭数据库连接
            }
        });

        //将准时的和迟到的UNION到一起
        DataStream<Tuple2<OrderDetail, OrderMain>> allOrderStream = punctualOrderDetailAndOrderMain.union(lateOrderDetailAndOrderMain);

        //根据具体的场景，写入到Kafka、Hbase、ES、ClickHouse
        allOrderStream.print();

        FlinkUtilsV2.getEnv().execute();
    }

    private static OrderMain queryOrderMainFromMySQL(Long order_id, Connection connection) {
        return null;
    }
}
