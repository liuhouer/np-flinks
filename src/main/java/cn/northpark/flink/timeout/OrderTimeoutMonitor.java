package cn.northpark.flink.timeout;

import cn.northpark.flink.table_sql_api.stream.sql.udf.UDF_Test;
import cn.northpark.flink.table_sql_api.stream.sql.udf.UserBrowseLog;
import com.alibaba.fastjson.JSON;
import lombok.extern.log4j.Log4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.InputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author liuhouer
 * @date 2020-10-25
 * Flink Java 订单(队列)超时取消(监控)的实现
 * <pre>
 *
 * 造数据：2分钟没支付就超时取消订单
 * {"userID": "user_5", "eventTime": "2020-10-25 09:46:47", "eventType": "create", "productID": "product_5", "productPrice": 20 ,"orderID":"1603349162707114"}
 *
 * {"userID": "user_5", "eventTime": "2020-10-25 09:49:11", "eventType": "pay", "productID": "product_5", "productPrice": 20 ,"orderID":"1603349162707114"}
 *
 * {"userID": "user_4", "eventTime": "2020-10-23 10:11:29", "eventType": "create", "productID": "product_5", "productPrice": 20 ,"orderID":"1603349424395132"}
 *
 * {"userID": "user_4", "eventTime": "2020-10-23 10:11:29", "eventType": "pay", "productID": "product_5", "productPrice": 20 ,"orderID":"1603349424395132"}
 *
 * </pre>
 */
@Log4j
public class OrderTimeoutMonitor {

    private static OutputTag<OrderResult> timeoutTag = new OutputTag<OrderResult>("OrderTimeout"){};

    public static void main(String[] args) throws Exception {
        //1、解析参数
        InputStream is = UDF_Test.class.getClassLoader().getResourceAsStream("config.properties");

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(is);
        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");
        String browseTopic = parameterTool.getRequired("browseTopic");
        String browseTopicGroupID = parameterTool.getRequired("browseTopicGroupID");


        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.setParallelism(1);

        //3、注册Kafka数据源
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers", kafkaBootstrapServers);
        browseProperties.put("group.id", browseTopicGroupID);
        DataStream<UserBrowseLog> browseStream = streamEnv
                .addSource(new FlinkKafkaConsumer<>(browseTopic, new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new BrowseBoundedOutOfOrdernessTimestampExtractor(Time.seconds(0)));

        SingleOutputStreamOperator<String> timeOutedStream = browseStream.keyBy(UserBrowseLog::getOrderID).process(new OrderTimeoutFunction());


        timeOutedStream.print();

        //测流输出超时未处理消息告警
        //队列超时监控的实现
        timeOutedStream.getSideOutput(timeoutTag).print();


        streamEnv.execute("TimeoutMonitor");

    }


    /**
     * 解析Kafka数据
     */
    static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserBrowseLog> out) throws Exception {
            try {

                UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getEventTime(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.setEventTimeTimestamp(eventTimeTimestamp);

                out.collect(log);
            } catch (Exception ex) {
                log.error("解析Kafka数据异常...", ex);
            }
        }
    }


    /**
     * 提取时间戳生成水印
     */
    static class BrowseBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserBrowseLog> {

        BrowseBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserBrowseLog element) {
            return element.getEventTimeTimestamp();
        }
    }


    static class OrderTimeoutFunction extends ProcessFunction<UserBrowseLog, String> {

        public static final int MAX_DELAY_TIMEOUT = 2;

        /**
         * 这个状态是通过 ProcessFunction 维护
         */
        private ValueState<Boolean> isPayedState;
        private ValueState<Long> timerState;

        private String orderID;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<>("isPayedState", Boolean.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerState", Long.class));
        }

        @Override
        public void processElement(UserBrowseLog value, Context ctx, Collector<String> out) throws Exception {

            orderID = value.getOrderID();
            Boolean isPayed = isPayedState.value();
            Long timerTs = timerState.value();

            if (value.getEventType().equals("create")) {  // 到来的事件是未处理事件
                // 乱序行为，先到pay再到create
                if (isPayed != null && isPayed) {
                    out.collect("payed successfully");
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                    isPayedState.clear();
                    timerState.clear();
                } else {
                    // 已创建订单未支付,设置定时器
                    Long ts = value.getEventTimeTimestamp() + MAX_DELAY_TIMEOUT * 60 * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerState.update(ts);
                }
            } else if (value.getEventType().equals("pay")) {
                //假如有定时器，说明create过
                if (timerTs!=null && timerTs > 0) {
                    // timerTs 是 认为超时后的时间戳
                    if (timerTs > value.getEventTimeTimestamp()) {
                        out.collect("订单号："+value.getOrderID()+"--支付成功");
                    } else {
                        ctx.output(timeoutTag, new OrderResult(value.getOrderID(), "订单已超时未支付，支付时长已过期"));
                    }
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                    isPayedState.clear();
                    timerState.clear();
                } else {
                    // 先来pay,还未创建订单
                    isPayedState.update(true);
                    // 等待设置的超时时长的时间
                    Long ts = value.getEventTimeTimestamp() + MAX_DELAY_TIMEOUT * 60 * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerState.update(ts);
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            Boolean isPayed = isPayedState.value();
            if (isPayed != null && isPayed) {
                ctx.output(timeoutTag, new OrderResult(orderID, "已支付但是未创建订单" ));
            } else {
                // 典型场景，只create,没有pay
                ctx.output(timeoutTag, new OrderResult(orderID, "超时未支付订单" ));
            }

            isPayedState.clear();
            timerState.clear();

        }

    }

    static class OrderResult implements Serializable {

        public OrderResult() {
        }

        public String orderID;
        public String resultMsg;

        public OrderResult(String orderID, String resultMsg) {
            this.orderID = orderID;
            this.resultMsg = resultMsg;
        }

        @Override
        public String toString() {
            return "OrderResult{" +
                    "orderID='" + orderID + '\'' +
                    ", resultMsg='" + resultMsg + '\'' +
                    '}';
        }
    }
}


/**

 OrderResult{orderID='1603349424395132', resultMsg='已支付但是未创建订单'}
 OrderResult{orderID='1603349424395132', resultMsg='超时未支付订单'}

 */