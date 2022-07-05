package cn.northpark.flink.vehicleAPP;

import cn.northpark.flink.ReadKafkaSinkMysql;
import cn.northpark.flink.util.TimeUtils;
import cn.northpark.flink.vehicleAPP.bean.GPSRealData;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
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
 * @date 2022年7月5日
 * 队列超过3分钟没来消息 输出到其他队列下线处理
 * 定时器+超时监控的实现（没涉及多判断，仅仅实现超时预警）
 * BRUCE TIPS!!!
 *
 */
@Slf4j
public class VehicleWithStatesHandleApp {
    static  final  String OFFLINE_TOPIC = "json-vehicle-offline";
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
        props.put("group.id", "bruce4");
        props.put("key.deserializer", parameters.getRequired("key.deserializer"));
        props.put("value.deserializer", parameters.getRequired("value.deserializer"));
        props.put("auto.offset.reset", parameters.getRequired("auto.offset.reset"));

        //2.read source
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(OFFLINE_TOPIC, new SimpleStringSchema(), props);


        DataStream<String> lines = env.addSource(kafkaSource);

        //3.transform-转化为实体
        SingleOutputStreamOperator<GPSRealData> map = lines.map(new MapFunction<String, GPSRealData>() {

            @Override
            public GPSRealData map(String value) throws Exception {

                log.info("flink_get_msg==="+ TimeUtils.getNowTime() +"====,{}",value);

                GPSRealData vo = JSON.parseObject(value, GPSRealData.class);

                return vo;
            }

        }).keyBy(new KeySelector<GPSRealData, Object>() {


            @Override
            public Object getKey(GPSRealData value) throws Exception {
                return value.getSimNo();
            }
        }).process(new ProcessFunction<GPSRealData, GPSRealData>() {

            private transient ValueState<Long> state;
            private transient ValueState<GPSRealData> stateM;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("time-state", Types.LONG);
                state = getRuntimeContext().getState(descriptor);

                ValueStateDescriptor<GPSRealData> descriptor1 = new ValueStateDescriptor<GPSRealData>("M-state", GPSRealData.class);
                stateM = getRuntimeContext().getState(descriptor1);

            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void processElement(GPSRealData value, Context ctx, Collector<GPSRealData> out) throws Exception {

                //假设数据的sendTime就是我们要计算的3分钟间隔字段
                Long sendTime = value.getSendTime();

                Long trigTime  = sendTime + 3* 60 * 1000L;


                // 注册一个定时器
                ctx.timerService().registerProcessingTimeTimer(trigTime);

                state.update(trigTime);

                stateM.update(value);

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<GPSRealData> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                //超过3分钟没有新数据过来 ，当前数据发送到下线的kafka topic
                //todo  往下线kafka的topic发消息
                Long state_val = state.value();

                if(state_val == timestamp){
                    log.error("=======");
                    log.error(TimeUtils.getNowTime());
                    log.error("=======");
                    log.error("已超时==={}",state);
                    log.error("已超时==={}",out);
                    out.collect(stateM.value());
                }


            }
        });


        //4.execute
        env.execute("VehicleOfflineHandleApp");

    }
}
