package cn.northpark.flink;


import cn.northpark.flink.bean.Message;
import cn.northpark.flink.table_sql_api.stream.sql.udf.UDTF_Test;
import cn.northpark.flink.util.HttpGetUtils;
import cn.northpark.flink.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author bruce
 * NorthPark 处理电影消息
 */
@Slf4j
public class NorthParkMovieHandler {

    public static void main(String[] args) throws Exception {

        //1、解析参数
        InputStream is = UDTF_Test.class.getClassLoader().getResourceAsStream("config.properties");

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(is);
        String npKafkaBootstrapServers = parameterTool.getRequired("npKafkaBootstrapServers");
        String npTopic = parameterTool.getRequired("npTopic");
        String npTopicGroupID = parameterTool.getRequired("npTopicGroupID");

        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //3、注册Kafka数据源
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers",npKafkaBootstrapServers);
        browseProperties.put("group.id",npTopicGroupID);

        streamEnv.addSource(new FlinkKafkaConsumer<>(npTopic, new SimpleStringSchema(), browseProperties))
                .process(new ProcessFunction<String, Message>() {
                    @Override
                    public void processElement(String s, Context context, Collector<Message> collector) throws Exception {
                        Message msg = JsonUtil.json2object(s,Message.class);

                        if(StringUtils.equals("movie",msg.getMsgType())){
                            log.info("爬取的数据----》,{}", msg.getBody());
                            String url = "https://northpark.cn/ret/movies/data";
                            String result = HttpGetUtils.sendPostJsonData(url, msg.getBody());
                            while (!result.contains("200")){
                                result = HttpGetUtils.sendPostJsonData(url, msg.getBody());
                            }
                        }
                        collector.collect(msg);
                    }
                });

        streamEnv.execute("NorthParkMovieHandler");

    }

}
