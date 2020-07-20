package cn.northpark.flink.table_sql_api.stream.sql.udf;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * UDTF一列转多列
 * UDTF，自定义表函数，继承TableFunction抽象类，主要实现eval方法。
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
public class UDTF_Test {
    public static void main(String[] args) throws Exception{

        //1、解析参数
        InputStream is = UDTF_Test.class.getClassLoader().getResourceAsStream("config.properties");

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

        //4、注册自定义函数
        tableEnv.registerFunction("UDTFOneColumnToMultiColumn",new UDTFOneColumnToMultiColumn());

        ///5、运行SQL
        String sql = ""
                + "select "
                + "	userID,eventTime,eventTimeTimestamp,eventType,productID,productPrice,rowtime,date1,time1 "
                + "from source_kafka ,"
                + "lateral table(UDTFOneColumnToMultiColumn(eventTime)) as T(date1,time1)";

        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table,Row.class).print();

        //6、开始执行
        tableEnv.execute(UDTF_Test.class.getSimpleName());


    }

    /**
     * 自定义UDTF
     * 将一列变成两列。
     * 如:2019-12-01 10:02:06 转换成date1(2019-12-01)和time1(10:02:06)两列。
     */
    public static class UDTFOneColumnToMultiColumn extends TableFunction<Row> {
        public void eval(String value) {
            String[] valueSplits = value.split(" ");

            //一行，两列
            Row row = new Row(2);
            row.setField(0,valueSplits[0]);
            row.setField(1,valueSplits[1]);
            collect(row);
        }
        @Override
        public TypeInformation<Row> getResultType() {
            return new RowTypeInfo(Types.STRING,Types.STRING);
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

//打印的结果 user_4,2019-12-01 10:02:16,1575165736000,browse,product_5,20,2019-12-01 02:02:16.0,2019-12-01,10:02:16
