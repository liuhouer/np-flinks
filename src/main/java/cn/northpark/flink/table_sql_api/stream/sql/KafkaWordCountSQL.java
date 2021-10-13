package cn.northpark.flink.table_sql_api.stream.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * 读取kafka数据 利用sql api来查询
 */
public class KafkaWordCountSQL {
    public static void main(String[] args) throws Exception {

        //实时dataStream api
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //实时Table执行上下文
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("bruce")
                .startFromEarliest()
                .property("bootstrap.servers","localhost:9092")

        ).withFormat(new Json().deriveSchema())
            .withSchema(new Schema()
                                   .field("name", TypeInformation.of(String.class))
                                   .field("gender",TypeInformation.of(String.class))
            ).inAppendMode().createTemporaryTable("kafkaSource");



        //这里是table api的实现写法
        Table table = tableEnv.scan("kafkaSource")
                .groupBy("gender")
                .select("gender ,count(1) as counts");

        tableEnv.toAppendStream(table, Row.class).print();
//        tableEnv.toRetractStream(table,Row.class).print();
        env.execute("KafkaWordCountSQL");
    }
}
