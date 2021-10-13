package cn.northpark.flink.table_sql_api.stream.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 自定义UDF 一行输入  一行输出
 */
public class UDFSQL {
    public static void main(String[] args) throws Exception {

        //实时dataStream api
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //注册一个可以cache的文件，通过网络发送给taskManager
        env.registerCachedFile("/Users/bruce/Desktop/ip.txt","ip-rules");

        //实时Table执行上下文
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //42.57.88.186
        //106.121.4.223
        DataStreamSource<String> lines = env.socketTextStream("localhost", 4000);

        tableEnv.registerDataStream("t_lines",lines,"ip");

        //注册自定义函数是一个UDF,输入一个IP地址，返回ROW<省、市>
        tableEnv.registerFunction("ipLocation",new IpLocation());

        Table table = tableEnv.sqlQuery("select ip,ipLocation(ip) from t_lines ");

        tableEnv.toAppendStream(table, Row.class).print();
//        tableEnv.toRetractStream(table,Row.class).print();
        env.execute("UDFSQL");
    }
}
