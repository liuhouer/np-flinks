package cn.northpark.flink.vehicleAPP;

import cn.northpark.flink.ReadKafkaSinkMysql;
import cn.northpark.flink.vehicleAPP.bean.GPSRealData;
import cn.northpark.flink.util.JDBCHelper;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import redis.clients.jedis.Jedis;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @author bruce
 * @date 2022年07月01日 11:11:05
 * flink-1 ：消费kafka数据实时写入Redis缓存<SimNo,GpsRealData>
 */
@Slf4j
public class SinkRedisMySQLApp1 {
    public static void main(String[] args) throws Exception{

        //1.环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //load config infos
        InputStream is = ReadKafkaSinkMysql.class.getClassLoader().getResourceAsStream("kafka.properties");

        ParameterTool parameters = ParameterTool.fromPropertiesFile(is);


        //将ParameterTool的参数设置成全局的参数
        env.getConfig().setGlobalJobParameters(parameters);


        //###############定义消费kafka source##############
        Properties props = new Properties();
        props.put("bootstrap.servers",parameters.getRequired("bootstrap.servers"));
        //props.put("zookeeper.connect", parameters.getRequired("zookeeper.connect"));
        props.put("group.id", parameters.getRequired("group.id"));
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

        }).filter(new FilterFunction<GPSRealData>() {
            @Override
            public boolean filter(GPSRealData value) throws Exception {
                return StringUtils.isNotBlank(value.getSimNo());
            }
        });


        //transform--按照SimNo keyby
        KeyedStream<GPSRealData, String> gpsRealDataStringKeyedStream = map.keyBy(new KeySelector<GPSRealData, String>() {
            @Override
            public String getKey(GPSRealData value) throws Exception {
                return value.getSimNo();
            }
        });

        //写入redis
        gpsRealDataStringKeyedStream.addSink(new RichSinkFunction<GPSRealData>() {

            private transient Jedis jedis = null;


            final String REDIS_PREFIX = "FLINK_RDF_GPS_REAL_DATA";

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                //获取redis链接
                ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                String host = params.getRequired("redis.host");
                String password = params.get("redis.pwd", null);
                int db = params.getInt("redis.db", 0);
                jedis = new Jedis(host, 6379, 5000);
                jedis.auth(password);
                jedis.select(db);
            }



            //插入或者更新redis
            @Override
            public void invoke(GPSRealData value, Context context) throws Exception {

                //FLINK_RDF_GPS_REAL_DATA <SimNo,GpsRealData>

                jedis.hset(REDIS_PREFIX, value.getSimNo(), JSON.toJSONString(value));

            }


            @Override
            public void close() throws Exception {
                super.close();
                if(jedis!=null){
                    jedis.close();
                }
            }
        });


        //写入mysql
        gpsRealDataStringKeyedStream.addSink(new RichSinkFunction<GPSRealData>() {
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
            public void invoke(GPSRealData value, Context context) throws Exception {
                String upsert_sql = "INSERT INTO flink.t_gps_real_data\n" +
                        "(alarm_state, altitude, area_alarm, area_id, area_type, dep_id, direction, dvr_status, gas, id, latitude, location," +
                        " longitude, mileage, online, online_date, over_speed_area_id, over_speed_area_type, parking_time, plate_no," +
                        " record_velocity, response_sn, route_alarm_type, route_segment_id, run_time_on_route, send_time, " +
                        " signal_state, sim_no, status, tired_alarm_time, update_date, valid, vehicle_id, velocity)" +
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                JDBCHelper.executeUpdate(connection,upsert_sql,
                        value.getAlarmState(),
                        value.getAltitude(),
                        value.getAreaAlarm() ,
                        value.getAreaId(),
                        value.getAreaType(),
                        value.getDepId(),
                        value.getDirection(),
                        value.getDvrStatus(),
                        value.getGas(),
                        value.getId(),
                        value.getLatitude(),
                        value.getLocation(),
                        value.getLongitude(),
                        value.getMileage(),
                        value.getOnline(),
                        value.getOnlineDate(),
                        value.getOverSpeedAreaId(),
                        value.getOverSpeedAreaType(),
                        value.getParkingTime(),
                        value.getPlateNo(),
                        value.getRecordVelocity(),
                        value.getResponseSn(),
                        value.getRouteAlarmType(),
                        value.getRouteSegmentId(),
                        value.getRunTimeOnRoute(),
                        value.getSendTime(),
                        value.getSignalState(),
                        value.getSimNo(),
                        value.getStatus(),
                        value.getTiredAlarmTime(),
                        value.getUpdateDate(),
                        value.getValid(),
                        value.getVehicleId(),
                        value.getVelocity());
            }
        });

        //4.execute
        env.execute("Kafka2RedisApp1");

    }
}
