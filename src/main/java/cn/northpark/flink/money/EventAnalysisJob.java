package cn.northpark.flink.money;

import cn.northpark.flink.money.bean.EventMsg;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author bruce
 * @date 2024年08月21日 17:46:48
 */
@Slf4j
public class EventAnalysisJob {
    public static void main(String[] args) throws Exception {

        //2、设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //默认的重启策略是无限重启  Integer.MAX_VALUE 次
        env.setParallelism(1);

        // Kafka consumer 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092");
        properties.setProperty("group.id", "bruce");
        properties.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("event000", new SimpleStringSchema(), properties);

        // 从Kafka读取数据
        DataStream<String> stream = env.addSource(consumer);

        // 解析JSON数组并展开
        DataStream<EventMsg> eventStream = stream.flatMap(new FlatMapFunction<String, EventMsg>() {
            @Override
            public void flatMap(String value, Collector<EventMsg> out) throws Exception {

                System.err.println("接收到消息++++"+value);
                log.info("接收到消息++++"+value);
                JSONArray jsonArray = JSON.parseArray(value);
                for (int i = 0; i < jsonArray.size(); i++) {
                    EventMsg eventMsg = JSON.parseObject(jsonArray.getString(i), EventMsg.class);
                    out.collect(eventMsg);
                }
            }
        });

        // 转换EventMsg为所需的字段
        DataStream<Tuple6<String, String, Integer, String, Integer, Long>> resultStream = eventStream.map(new MapFunction<EventMsg, Tuple6<String, String, Integer, String, Integer, Long>>() {
            @Override
            public Tuple6<String, String, Integer, String, Integer, Long> map(EventMsg eventMsg) throws Exception {
                return new Tuple6<>(
                        eventMsg.getDistinctId(),
                        eventMsg.getEvent(),
                        eventMsg.getProperties().getTimeAt(),
                        eventMsg.getProperties().getTitle(),
                        eventMsg.getProperties().getUserId(),
                        eventMsg.getTime()
                );
            }
        });


        resultStream.print();

        // 将DataStream转换为Table
        Table eventTable = tableEnv.fromDataStream(resultStream,
                $("distinct_id"),
                $("event_name"),
                $("time_at"),
                $("page_title"),
                $("user_id"),
                $("event_time")
        );

        // 注册表
        tableEnv.createTemporaryView("events", eventTable);


        // 创建StarRocks event_summary表
        tableEnv.executeSql(
                "CREATE TABLE event_summary3 (" +
                        "id STRING, " +
                        "event_name STRING, " +
                        "page_title STRING, " +
                        "user_id INT, " +
                        "time_at INT, " +
                        "event_date DATE," +
                        "event_hour BIGINT," +
                        "event_time BIGINT" +
                        ")  WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = 'jdbc:mysql://node1:9030/flink'," +
                        "'table-name' = 'event_summary4'," +
                        "'username' = 'root'," +
                        "'password' = ''," +
                        "'driver' = 'com.mysql.cj.jdbc.Driver'" +
                        ")"
        );

        // 插入StarRocks
        tableEnv.executeSql("INSERT INTO event_summary3 SELECT distinct_id as id ,event_name, page_title, user_id, time_at, " +
                "CAST(TO_TIMESTAMP(FROM_UNIXTIME(event_time/1000)) AS DATE) AS event_date, " +
                "EXTRACT(HOUR FROM TO_TIMESTAMP(FROM_UNIXTIME(event_time/1000))) AS event_hour, " +
                "event_time " +
                "FROM events");

        env.execute("EventAnalysisJob");
    }


}
