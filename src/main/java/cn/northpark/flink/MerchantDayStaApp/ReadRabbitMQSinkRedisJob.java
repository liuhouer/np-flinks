package cn.northpark.flink.MerchantDayStaApp;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import redis.clients.jedis.Jedis;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author bruce
 * @date 2022年05月08日 22:58:17
 *
 * DDL:
 * -- flink.t_merchant_day_sta definition
 *
 * CREATE TABLE `t_merchant_day_sta` (
 *   `id` varchar(10) NOT NULL,
 *   `merchant_id` varchar(20) NOT NULL,
 *   `total_deduct_money` decimal(38,2) NOT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 * INSERT INTO flink.t_merchant_day_sta (id,merchant_id,total_deduct_money) VALUES
 * ('1','1001',10.22)
 * ,('2','1001',25.66)
 * ,('3','1002',888.64)
 * ;
 *
 *
 */
public class ReadRabbitMQSinkRedisJob {
    public static void main(String[] args) throws Exception {
        // 1,执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        //load config infos
        InputStream is = ReadRabbitMQSinkRedisJob.class.getClassLoader().getResourceAsStream("config.properties");

        ParameterTool parameters = ParameterTool.fromPropertiesFile(is);

        //将ParameterTool的参数设置成全局的参数
        env.getConfig().setGlobalJobParameters(parameters);


        //只有开启了checkpoint 才会有重启策略 默认是不重启
        env.enableCheckpointing(60000);//每隔5s进行一次checkpoint
        //默认的重启策略是无限重启  Integer.MAX_VALUE 次

        //重启重试次数
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 2000));


        // 2,RabbitMQ配置 ----read from config
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(parameters.getRequired("mq.host"))
                .setPort(parameters.getInt("mq.port"))
                .setUserName(parameters.getRequired("mq.user"))
                .setPassword(parameters.getRequired("mq.pass"))
                .setVirtualHost(parameters.getRequired("mq.VirtualHost"))
                .build();

        // 3,添加资源
        DataStreamSource<String> dataStreamSource = env.addSource(new RMQSource<String>(
                connectionConfig,
                parameters.getRequired("mq.amount.queueName"),
                false,
                new SimpleStringSchema())).setParallelism(1);


        // 4，实体转化 + 统计金额累加
        SingleOutputStreamOperator<MerchantDaySta> sumed = dataStreamSource.map(new MapFunction<String, MerchantDaySta>() {
            @Override
            public MerchantDaySta map(String s) throws Exception {
                MerchantDaySta bean = JSON.parseObject(s, MerchantDaySta.class);
                return bean;
            }
        }).keyBy(MerchantDaySta::getMerchantId).sum("totalDeductMoney");

        sumed.print();

        //sink to redis
        sumed.addSink(new RichSinkFunction<MerchantDaySta>() {

            private transient Jedis jedis = null;

            private transient Connection connection = null;

            final String REDIS_PREFIX = "FLINK_TOTAL_DEDUCT_MONEY";

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


            @Override
            public void invoke(MerchantDaySta value, Context context) throws Exception {
                //1.从hash Hexists 命令
                String merchantId = value.getMerchantId();
                Boolean hexists = jedis.hexists(REDIS_PREFIX, merchantId);
                //2.key存在，拿出来追加,覆盖值
                if(hexists){
                    String oldVal = jedis.hget(REDIS_PREFIX, merchantId);
                    Double newVal = value.getTotalDeductMoney()+Double.valueOf(oldVal);
                    jedis.hset(REDIS_PREFIX,merchantId,newVal.toString());
                }else{
                    //3.获取数据库的值


                    //获取mysql链接
                    ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                    String driverClassName = params.getRequired("driverClassName");
                    String jdbcUrl = params.getRequired("jdbcUrl");
                    String username = params.getRequired("username");
                    String password = params.getRequired("password");

                    connection = DriverManager.getConnection(jdbcUrl,username,password);

                    PreparedStatement preparedStatement = connection.prepareStatement(" SELECT  merchant_id, sum(total_deduct_money) as total_deduct_money  " +
                            " FROM t_merchant_day_sta where merchant_id = ? group by merchant_id ");

                    preparedStatement.setString(1,value.getMerchantId());

                    ResultSet resultSet = preparedStatement.executeQuery();
                    Double oldVal = 0d;
                    while (resultSet.next()){
                        oldVal = resultSet.getDouble(1);
                    }

                    Double newVal = value.getTotalDeductMoney()+oldVal;
                    jedis.hset(REDIS_PREFIX,merchantId,newVal.toString());


                }

            }

            @Override
            public void close() throws Exception {
                super.close();
                if(jedis!=null){
                    jedis.close();
                }
                if(connection!=null){
                    connection.close();
                }
            }


        });


        //execute
        env.execute("InstructionProcessJob");

    }
}
