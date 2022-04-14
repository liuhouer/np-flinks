package cn.northpark.flink.join;

import cn.northpark.flink.util.FlinkUtilsV2;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author bruce
 * @date 2022年04月14日 10:44:04
 *
 *
 * 在各种各样的系统中，都订单数据表
 *     订单表：订单主表、订单明细表
 *
 *     订单主表：
 *         订单id、订单状态、订单总金额、订单的时间、用户ID
 *
 *     订单明细表：
 *         订单主表的ID、商品ID、商品的分类ID、商品的单价、商品的数量
 *
 * 统计某个分类的成交金额
 *     订单状态为成交的（主表中）
 *     商品的分类ID（明细表中）
 *     商品的金额即单价*数量（明细表中）
 *
 * 在京东或淘宝，下来一个单
 *     o1000，已支付，600,2020-06-29 19:42:00，feng
 *     p1, 100, 1, 食品
 *     p1, 200, 1, 食品
 *     p1, 300, 1, 食品
 */
public class OrderJoin {
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

        //Left Out JOIN，并且将订单明细表作为左表
        DataStream<Tuple2<OrderDetail, OrderMain>> joined = orderDetailStreamWithWaterMark.coGroup(orderMainStreamWithWaterMark)
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
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
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

        joined.print();

        FlinkUtilsV2.getEnv().execute();
    }
}
