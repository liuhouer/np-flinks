package cn.northpark.flink.table_sql_api.stream.sql.udf;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * UDF: 自定义标量函数(User Defined Scalar Function)。一行输入一行输出。
 *
 * <pre>
 *
 * // 某个用户在某个时刻浏览了某个商品，以及商品的价值
 * // eventTime: 北京时间，方便测试。如下，乱序数据:
 * {"userID": "user_5", "eventTime": "2019-12-01 10:02:00", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_4", "eventTime": "2019-12-01 10:02:02", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_5", "eventTime": "2019-12-01 10:02:06", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_4", "eventTime": "2019-12-01 10:02:10", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_5", "eventTime": "2019-12-01 10:02:06", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_5", "eventTime": "2019-12-01 10:02:06", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_4", "eventTime": "2019-12-01 10:02:12", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_5", "eventTime": "2019-12-01 10:02:06", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_5", "eventTime": "2019-12-01 10:02:06", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_4", "eventTime": "2019-12-01 10:02:15", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 * {"userID": "user_4", "eventTime": "2019-12-01 10:02:16", "eventType": "browse", "productID": "product_5", "productPrice": 20}
 *
 * </pre>
 */
@Slf4j
public class UDF_Test {
    public static void main(String[] args) throws Exception{

        //1、解析参数
        InputStream is = UDF_Test.class.getClassLoader().getResourceAsStream("config.properties");

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(is);
        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");
        String browseTopic = parameterTool.getRequired("browseTopic");
        String browseTopicGroupID = parameterTool.getRequired("browseTopicGroupID");

        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        streamEnv.setParallelism(1);

        //3、注册Kafka数据源
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers",kafkaBootstrapServers);
        browseProperties.put("group.id",browseTopicGroupID);
        DataStream<UserBrowseLog> browseStream=streamEnv
                .addSource(new FlinkKafkaConsumer<>(browseTopic, new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new BrowseBoundedOutOfOrdernessTimestampExtractor(Time.seconds(5)));

        // 增加一个额外的字段rowtime为事件时间属性
        tableEnv.registerDataStream("source_kafka",browseStream,"userID,eventTime,eventTimeTimestamp,eventType,productID,productPrice,rowtime.rowtime");

        //4、注册UDF
        //日期转换函数: 将Flink Window Start/End Timestamp转换为指定时区时间(默认转换为北京时间)
        tableEnv.registerFunction("UDFTimestampConverter", new UDFTimestampConverter());

        //5、运行SQL
        //基于事件时间，maxOutOfOrderness为5秒，滚动窗口，计算10秒内每个商品被浏览的PV
        String sql = ""
                + "	select "
                + "		UDFTimestampConverter(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'YYYY-MM-dd HH:mm:ss') as window_start, "
                + "		UDFTimestampConverter(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'YYYY-MM-dd HH:mm:ss','+08:00') as window_end, "
                + "		productID, "
                + "		count(1) as browsePV"
                + "	from source_kafka "
                + "	group by productID,TUMBLE(rowtime, INTERVAL '10' SECOND)";

        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table,Row.class).print();

        //6、开始执行
        tableEnv.execute(UDF_Test.class.getSimpleName());


    }

    /**
     * 自定义UDF
     */
    public static class UDFTimestampConverter extends ScalarFunction{

        /**
         * 默认转换为北京时间
         * @param timestamp Flink Timestamp 格式时间
         * @param format 目标格式,如"YYYY-MM-dd HH:mm:ss"
         * @return 目标时区的时间
         */
        public String eval(Timestamp timestamp,String format){

            LocalDateTime noZoneDateTime = timestamp.toLocalDateTime();
            ZonedDateTime utcZoneDateTime = ZonedDateTime.of(noZoneDateTime, ZoneId.of("UTC"));

            ZonedDateTime targetZoneDateTime = utcZoneDateTime.withZoneSameInstant(ZoneId.of("+08:00"));

            return targetZoneDateTime.format(DateTimeFormatter.ofPattern(format));
        }

        /**
         * 转换为指定时区时间
         * @param timestamp Flink Timestamp 格式时间
         * @param format 目标格式,如"YYYY-MM-dd HH:mm:ss"
         * @param zoneOffset 目标时区偏移量
         * @return 目标时区的时间
         */
        public String eval(Timestamp timestamp,String format,String zoneOffset){

            LocalDateTime noZoneDateTime = timestamp.toLocalDateTime();
            ZonedDateTime utcZoneDateTime = ZonedDateTime.of(noZoneDateTime, ZoneId.of("UTC"));

            ZonedDateTime targetZoneDateTime = utcZoneDateTime.withZoneSameInstant(ZoneId.of(zoneOffset));

            return targetZoneDateTime.format(DateTimeFormatter.ofPattern(format));
        }
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
            }catch (Exception ex){
                log.error("解析Kafka数据异常...",ex);
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
}

//打印的结果 2019-12-01 10:02:00,2019-12-01 10:02:10,product_5,6
