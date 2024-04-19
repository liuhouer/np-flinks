package cn.northpark.flink.clickhouse;

import cn.northpark.flink.ReadKafkaSinkMysql;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author bruce
 * @date 2024年04月18日 17:37:03
 */
@Slf4j
public class KafkaWriteClickHouse {
    public static void main(String[] args) throws Exception{
        //1.环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //load config infos
        InputStream is = ReadKafkaSinkMysql.class.getClassLoader().getResourceAsStream("kafka.properties");

        ParameterTool parameters = ParameterTool.fromPropertiesFile(is);

        final String ckurl = parameters.getRequired("ck.url");
        final String ckuser = parameters.getRequired("ck.user");
        final String ckpass = parameters.getRequired("ck.pass");

        env.setParallelism(16); // 设置并行度

        //###############定义消费kafka source##############
        Properties props = new Properties();
        props.put("bootstrap.servers",parameters.getRequired("bootstrap.servers"));
        //props.put("zookeeper.connect", parameters.getRequired("zookeeper.connect"));
        props.put("group.id", parameters.getRequired("group.id"));
        props.put("key.deserializer", parameters.getRequired("key.deserializer"));
        props.put("value.deserializer", parameters.getRequired("value.deserializer"));
        props.put("auto.offset.reset", parameters.getRequired("auto.offset.reset"));

        //2.read source
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("drg_pay", new SimpleStringSchema(), props);


        env.addSource(kafkaSource).process(new ProcessFunction<String, Object>() {
            private transient Connection connection;
            private transient PreparedStatement statement;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 创建 ClickHouse 连接
                connection = DriverManager.getConnection(ckurl,ckuser,ckpass);
            }

            @Override
            public void close() throws Exception {
                super.close();
                // 关闭连接和预编译语句
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }

            @Override
            public void processElement(String s, Context context, Collector<Object> collector) throws Exception {

                try {
                    // 准备插入语句的预编译语句
                    statement = connection.prepareStatement(s);
                    // 执行插入操作

                    String msg = statement.executeUpdate() > 0 ? "插入成功..."
                            : "插入失败...";
                    //connection.commit();
                    log.info(msg);
                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        });


        //4.execute
        env.execute("KafkaWriteClickHouse");


    }
}
