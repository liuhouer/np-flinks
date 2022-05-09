package cn.northpark.flink.table_sql_api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author bruce
 * @date 2022年05月08日 22:38:06
 */
public class Covid19Job {
    public static final Boolean UPDATE_MODE = true;
    public static final Boolean INSERT_MODE = false;

    public static void main(String[] args) throws Exception {


        //1、解析参数
        final String ZK_PATH="node1:2181,node2:2181,node3:2181";
        final String KAFKA_PATH="node1:9092,node2:9092,node3:9092";


        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =  EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv,settings);


        //只有开启了checkpoint 才会有重启策略 默认是不重启
        // streamEnv.enableCheckpointing(30000, CheckpointingMode.AT_LEAST_ONCE);//每隔5s进行一次checkpoint

        //默认的重启策略是无限重启  Integer.MAX_VALUE 次
        streamEnv.setParallelism(1);

        //3、转化消息，发送到消息队列
        String readTopic = "covid19";


        //4、 创建kafka数据源表，如下：不能是包含业务信息的message
        //经测试：kafka数据源映射类型不能为decimal和timestamp
        //id	lastUpdateTime	name	total_confirm	total_suspect	total_heal	total_dead	total_severe
        // total_input	total_newConfirm	total_newDead	total_newHeal	today_confirm	today_suspect	today_heal
        // today_dead	today_severe	today_storeConfirm
        String T_COVID = "CREATE TABLE T_COVID (" +

                "  id                STRING  , " +
                "  lastUpdateTime                 STRING , " +
                "  name            STRING  , " +
                "  total_confirm                 STRING  , " +
                "  total_suspect               STRING  , " +
                "  total_heal       STRING  , " +
                "  total_dead       STRING  , " +
                "  total_severe        STRING  , " +
                "  total_input                STRING  , " +
                "  total_newConfirm                 STRING  , " +
                "  total_newDead           STRING  , " +
                "  total_newHeal             STRING  , " +
                "  today_confirm                STRING, " +
                "  today_suspect            STRING  , " +
                "  today_heal         STRING  , " +
                "  today_dead         STRING  , " +
                "  today_severe            STRING, " +
                "  today_storeConfirm          STRING   " +
//                "  storeConfirm          STRING  , " +//现存确诊
//                "  deadRate              DOUBLE   " +//病死率
                //"   PROCTIME AS PROCTIME() " +
                " ) WITH (" +

                "   'connector.type' = 'kafka'," +
                "   'connector.topic' = '" + readTopic + "'," +
                "   'connector.version' = 'universal'," +
                "   'connector.startup-mode' = 'latest-offset'," +
                "   'connector.properties.group.id' = 'bruce'," +
                "   'connector.properties.zookeeper.connect' = '"+ZK_PATH+"'," +
                "   'connector.properties.bootstrap.servers' = '"+KAFKA_PATH+"'," +
//                "   'update-mode' = 'append' ," +
                "   'format.type' = 'json'" +
                " ) ";


        tEnv.executeSql(T_COVID);

        //查询所有字段+现存确诊+病死率
        String getExec = "select *, CAST(" +
                "(" +
                "CAST(total_confirm AS INTEGER) - CAST(total_heal AS INTEGER)- CAST(total_dead AS INTEGER)" +
                ")" +
                "AS STRING" +
                ") as storeConfirm ," +
                "ROUND(CAST(total_dead AS DOUBLE)/CAST(total_confirm AS DOUBLE),2)" +
                "  as deadRate" +
                " from T_COVID  where total_confirm is not null and total_heal is not null and total_dead is not null";

        //打印到控制台
        tEnv.executeSql(getExec).print();

        //执行查询语句，返回table
        Table table = tEnv.sqlQuery(getExec);

        //定义输出到csv
        tEnv.connect(new FileSystem().path("C:\\Users\\Bruce\\Desktop\\result.csv"))
                .withFormat(new OldCsv().writeMode(org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE.name()))
                .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
                        .field("lastUpdateTime",DataTypes.STRING())
                        .field("name",DataTypes.STRING())
                        .field("total_confirm",DataTypes.STRING())
                        .field("total_suspect",DataTypes.STRING())
                        .field("total_heal",DataTypes.STRING())
                        .field("total_dead",DataTypes.STRING())
                        .field("total_severe",DataTypes.STRING())
                        .field("total_input",DataTypes.STRING())
                        .field("total_newConfirm",DataTypes.STRING())
                        .field("total_newDead",DataTypes.STRING())
                        .field("total_newHeal",DataTypes.STRING())
                        .field("today_confirm",DataTypes.STRING())
                        .field("today_suspect",DataTypes.STRING())
                        .field("today_heal",DataTypes.STRING())
                        .field("today_dead",DataTypes.STRING())
                        .field("today_severe",DataTypes.STRING())
                        .field("today_storeConfirm",DataTypes.STRING())
                        .field("storeConfirm",DataTypes.STRING())
                        .field("deadRate",DataTypes.DOUBLE()))
                .createTemporaryTable("csv");

        table.insertInto("csv");

        tEnv.execute("Covid19Job");


    }

}
