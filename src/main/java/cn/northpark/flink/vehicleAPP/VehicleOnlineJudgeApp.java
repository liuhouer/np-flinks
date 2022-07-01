package cn.northpark.flink.vehicleAPP;

import cn.northpark.flink.ReadKafkaSinkMysql;
import cn.northpark.flink.vehicleAPP.bean.GPSRealData;
import cn.northpark.flink.util.KafkaString;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author bruce
 * @date 2022年07月01日 11:11:05
 * flink-2 ：
 * 车辆上线判定：消费kafka数据，判断车辆的上一个GPSRealData是否上线，如果online=false, 则生成一条消息发送到topic: json-vehicle-online
 */
@Slf4j
public class VehicleOnlineJudgeApp {
    static  final  String ONLINE_TOPIC = "json-vehicle-online";
    public static void main(String[] args) throws Exception{

        //1.环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //load config infos
        InputStream is = ReadKafkaSinkMysql.class.getClassLoader().getResourceAsStream("kafka.properties");

        ParameterTool parameters = ParameterTool.fromPropertiesFile(is);


        //只有开启了checkpoint 才会有重启策略 默认是不重启
        env.enableCheckpointing(5000);//每隔5s进行一次checkpoint
        //默认的重启策略是无限重启  Integer.MAX_VALUE 次

        //重启重试次数
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));

        //将ParameterTool的参数设置成全局的参数
        env.getConfig().setGlobalJobParameters(parameters);


        //###############定义消费kafka source##############
        Properties props = new Properties();
        props.put("bootstrap.servers",parameters.getRequired("bootstrap.servers"));
        //props.put("zookeeper.connect", parameters.getRequired("zookeeper.connect"));
        //todo 同时消费rdf_topics的数据 ,进行上线判定,配置为不同的消费组
        props.put("group.id", "bruce2");
        props.put("key.deserializer", parameters.getRequired("key.deserializer"));
        props.put("value.deserializer", parameters.getRequired("value.deserializer"));
        props.put("auto.offset.reset", parameters.getRequired("auto.offset.reset"));

        //2.read source
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(parameters.getRequired("rdf_topics"), new SimpleStringSchema(), props);


        DataStream<String> lines = env.addSource(kafkaSource);

        //3.transform-转化为实体
        SingleOutputStreamOperator<GPSRealData> map = lines.map(new MapFunction<String, GPSRealData>() {

            @Override
            public GPSRealData map(String value) throws Exception {

                log.info("flink_get_msg===,{}",value);

                GPSRealData vo = JSON.parseObject(value, GPSRealData.class);

                return vo;
            }

        }).process(new ProcessFunction<GPSRealData, GPSRealData>() {
            @Override
            public void processElement(GPSRealData value, Context ctx, Collector<GPSRealData> out) throws Exception {
                if(!value.getOnline()){
                    //发送到json-vehicle-online的topic
                    Properties properties = KafkaString.buildBasicKafkaProperty();

                    KafkaString.sendKafkaString(properties, ONLINE_TOPIC,JSON.toJSONString(value));
                }
            }
        });


        //4.execute
        env.execute("VehicleOnlineJudgeApp");

    }
}
