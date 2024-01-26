package cn.northpark.flink;


import cn.northpark.flink.bean.Message;
import cn.northpark.flink.table_sql_api.stream.sql.udf.UDTF_Test;
import cn.northpark.flink.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
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

        //只有开启了checkpoint 才会有重启策略 默认是不重启
        streamEnv.enableCheckpointing(60000 * 60 );//每隔5s进行一次checkpoint
        //默认的重启策略是无限重启  Integer.MAX_VALUE 次

        //重启重试次数
//        streamEnv.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(3,2000));

        //设置状态存储的后端,一般写在flink的配置文件中
//        streamEnv.setStateBackend(new FsStateBackend("hdfs://node1:9000/np-backend"));
        streamEnv.setStateBackend(new FsStateBackend("file:///home/crhms/app/flink-1.12.1/np-backend"));

        //程序异常退出或者人为cancel以后，不删除checkpoint数据
        streamEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //3、注册Kafka数据源
        Properties props = new Properties();
        props.put("bootstrap.servers",npKafkaBootstrapServers);
        props.put("group.id",npTopicGroupID);
//        props.setProperty("auto.offset.reset", "earliest") ;

        streamEnv.addSource(new FlinkKafkaConsumer<>(npTopic, new SimpleStringSchema(), props))
                .process(new ProcessFunction<String, Message>() {
                    @Override
                    public void processElement(String s, Context context, Collector<Message> collector) throws Exception {
                        try {

                            Message msg = JsonUtil.json2object(s,Message.class);

                            if(StringUtils.equals("movie",msg.getMsgType())){
                                log.info("爬取的数据----》,{}", msg.getBody());
                                String url = "https://northpark.cn/ret/movies/data";
                                boolean result = sendPostJsonData(url, msg.getBody());
                                while (!result){
                                    result = sendPostJsonData(url, msg.getBody());
                                }
                            }
                            collector.collect(msg);
                        }catch (Exception e){
                            log.error("e-----{}",e);
                            log.error("not the real Message");
                        }

                    }
                });

        streamEnv.execute("NorthParkMovieHandler");

    }


    public static boolean sendPostJsonData(String url, String json) {
        try {
            // 禁用SSL验证
            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial((chain, authType) -> true).build();
            SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);

            CloseableHttpClient httpclient = HttpClients.custom()
                    .setSSLSocketFactory(sslSocketFactory)
                    .build();

            HttpPost httpPost = new HttpPost(url);
            httpPost.addHeader("Content-Type", "application/json;charset=UTF-8");

            // 设置JSON参数
            StringEntity stringEntity = new StringEntity(json, "UTF-8");
            stringEntity.setContentEncoding("UTF-8");

            httpPost.setEntity(stringEntity);

            System.out.println("Executing request " + httpPost.getRequestLine());

            HttpResponse response = httpclient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            System.out.println("Response status code: " + statusCode);

            // 判断状态码是否为200
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                String responseBody = EntityUtils.toString(entity);
                System.out.println("Response body: " + responseBody);
                return true;
            } else {
                System.out.println("Unexpected response status: " + statusCode);
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }



}
