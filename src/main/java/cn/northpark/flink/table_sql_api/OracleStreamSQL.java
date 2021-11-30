package cn.northpark.flink.table_sql_api;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhangyang
 * @Date 2020年09月29日 17:38:27
 * <p>
 * flink直连oracle源表写sql统计结果【sql流式操作和纯sql操作2种方案】
 */
@Slf4j
public class OracleStreamSQL {

    public static final Boolean UPDATE_MODE = true;
    public static final Boolean INSERT_MODE = false;

    public static void main(String[] args) throws Exception {


        //1、解析参数
//        InputStream is = UDF_Test.class.getClassLoader().getResourceAsStream(EnvConstant.CONFIG_PROP);
//
//        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(is);
//        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");
//        String jdbcBondTopic = parameterTool.getRequired("JdbcBondTopic");
//        String jdbcBondGroupID = parameterTool.getRequired("JdbcBondGroupID");


        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        EnvironmentSettings settings =  EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv,settings);


        //只有开启了checkpoint 才会有重启策略 默认是不重启
       // streamEnv.enableCheckpointing(30000, CheckpointingMode.AT_LEAST_ONCE);//每隔5s进行一次checkpoint

        //默认的重启策略是无限重启  Integer.MAX_VALUE 次
//        streamEnv.setParallelism(1);


        String VERSION_TEST = " create table VERSION_TEST " +
                " ( " +
                "  ID                      DECIMAL , " +
                "  VERSION                 STRING , " +
                "  AREA_CODE               STRING, " +
                "  AREA_VERSION            STRING , " +
                "  PRIMARY KEY(ID)  NOT ENFORCED " + //设置入库主键以后，自动适配insert和update 测试通过2020-11-16 14:47:56

                " ) WITH (" +

                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:oracle:thin:@node1:1521:ora29a'," +
                "   'table-name' = 'VERSION_TEST'," +
                "   'username' = 'audit'," +
                "   'password' = 'audit'" +

                " )";

        
        tEnv.executeSql(VERSION_TEST);

        //1.流式查询操作
        Table table = tEnv.sqlQuery("select * from VERSION_TEST");

//        tEnv.toRetractStream(table,Row.class).print();

        tEnv.toRetractStream(table,Row.class).addSink(new RichSinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                Row f1 = value.f1;
                System.err.println(f1.getField(1).toString());
                log.info("########"+f1.getField(1).toString());
            }
        });


        //2.SQL查询操作
//        TableResult tableResult = tEnv.executeSql("select * from VERSION_TEST");
//
//        tableResult.print();



        //流式操作需要执行
        streamEnv.execute("OracleStreamSQL");


    }

}
