package cn.northpark.flink.vehicleAPP;

import cn.northpark.flink.ReadKafkaSinkMysql;
import cn.northpark.flink.vehicleAPP.bean.GPSRealData;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @author bruce
 * @date 2022年07月01日 11:11:05
 * flink-3 ：
 * topic: json-vehicle-online, 消费该topic生成一条车辆上线记录
 */
@Slf4j
public class VehicleOnlineHandleApp {
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
        props.put("group.id", "bruce3");
        props.put("key.deserializer", parameters.getRequired("key.deserializer"));
        props.put("value.deserializer", parameters.getRequired("value.deserializer"));
        props.put("auto.offset.reset", parameters.getRequired("auto.offset.reset"));

        //2.read source
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(ONLINE_TOPIC, new SimpleStringSchema(), props);


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
            private transient Connection connection = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                //获取mysql链接
                ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                String driverClassName = params.getRequired("driverClassName");
                String jdbcUrl = params.getRequired("jdbcUrl");
                String username = params.getRequired("username");
                String password = params.getRequired("password");
                connection = DriverManager.getConnection(jdbcUrl,username,password);

            }

            @Override
            public void close() throws Exception {
                super.close();
                if(connection!=null){
                    connection.close();
                }
            }

            @Override
            public void processElement(GPSRealData value, Context ctx, Collector<GPSRealData> out) throws Exception {

                //TODO 生成一条车辆上线记录的 业务逻辑写在这里 ,可参考如下的逻辑实现

                log.info("在这生成车辆上线记录,正在处理该条目===>{}",value);

                //String upsert_sql = "INSERT INTO flink.t_gps_real_data\n" +
                //                        "(alarm_state, altitude, area_alarm, area_id, area_type, dep_id, direction, dvr_status, gas, id, latitude, location," +
                //                        " longitude, mileage, online, online_date, over_speed_area_id, over_speed_area_type, parking_time, plate_no," +
                //                        " record_velocity, response_sn, route_alarm_type, route_segment_id, run_time_on_route, send_time, " +
                //                        " signal_state, sim_no, status, tired_alarm_time, update_date, valid, vehicle_id, velocity)" +
                //                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                //                JDBCHelper.executeUpdate(connection,upsert_sql,
                //                        value.getAlarmState(),
                //                        value.getAltitude(),
                //                        value.getAreaAlarm() ,
                //                        value.getAreaId(),
                //                        value.getAreaType(),
                //                        value.getDepId(),
                //                        value.getDirection(),
                //                        value.getDvrStatus(),
                //                        value.getGas(),
                //                        value.getId(),
                //                        value.getLatitude(),
                //                        value.getLocation(),
                //                        value.getLongitude(),
                //                        value.getMileage(),
                //                        value.getOnline(),
                //                        value.getOnlineDate(),
                //                        value.getOverSpeedAreaId(),
                //                        value.getOverSpeedAreaType(),
                //                        value.getParkingTime(),
                //                        value.getPlateNo(),
                //                        value.getRecordVelocity(),
                //                        value.getResponseSn(),
                //                        value.getRouteAlarmType(),
                //                        value.getRouteSegmentId(),
                //                        value.getRunTimeOnRoute(),
                //                        value.getSendTime(),
                //                        value.getSignalState(),
                //                        value.getSimNo(),
                //                        value.getStatus(),
                //                        value.getTiredAlarmTime(),
                //                        value.getUpdateDate(),
                //                        value.getValid(),
                //                        value.getVehicleId(),
                //                        value.getVelocity());

            }
        });


        //4.execute
        env.execute("VehicleOnlineHandleApp");

    }
}
