package cn.northpark.flink.table_sql_api.stream.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 自定义UDTF 一行输入  多行输出
 */
public class UDTFSQL {
    public static void main(String[] args) throws Exception {

        //实时dataStream api
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //实时Table执行上下文
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //42.57.88.186
        //a.b.c.d
        //聪.明.的.六.猴.儿
        DataStreamSource<String> lines = env.socketTextStream("localhost", 4000);

        tableEnv.registerDataStream("t_lines",lines,"line");

        //注册自定义函数是一个UDTF,输入一行字符串，返回多列
        tableEnv.registerFunction("split",new Split("\\."));

        //lateral:表生成函数
        Table table = tableEnv.sqlQuery("select word from t_lines,lateral table(split(line)) as T(word) ");

        tableEnv.toAppendStream(table, Row.class).print();
//        tableEnv.toRetractStream(table,Row.class).print();
        env.execute("UDTFSQL");
    }
}
