package cn.northpark.flink.starrocks;

import com.google.gson.Gson;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @author bruce
 * @date 2024年08月22日 16:29:56
 */
public class FlinkToStarRocksTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        FlinkToStarRocksTest ft = new FlinkToStarRocksTest();
        ft.useSql(tableEnv);
//        ft.useStream(env);
    }


    void useStream(StreamExecutionEnvironment env) {
        Gson gson = new Gson();
        DataStreamSource<String> studentDataStreamSource = env.fromCollection(Arrays.asList(gson.toJson(new Student(1001, "张三", 18, "111@qq.com", "男")),
                gson.toJson(new Student(1002, "李四", 19, "222@qq.com", "女")),
                gson.toJson(new Student(1003, "王五", 20, "333@qq.com", "男"))));
        studentDataStreamSource.addSink(StarRocksSink.sink(
                        StarRocksSinkOptions.builder()
                                .withProperty("jdbc-url", "jdbc:mysql://node1:9039") //fe query_port
                                .withProperty("load-url", "node1:8039")//FE http_port
                                .withProperty("username", "root")
                                .withProperty("password", "")
                                .withProperty("table-name", "flink_student")
                                .withProperty("database-name", "flink")
                                .withProperty("sink.properties.format", "json")
                                .withProperty("sink.properties.strip_outer_array", "true")
                                .build()
                )
        );

    }

    void useSql(StreamTableEnvironment env) {
        env.executeSql("CREATE TABLE flink_student(\n" +
                "                uid INT,\n" +
                "                name VARCHAR(20),\n" +
                "                age INT,\n" +
                "                email VARCHAR(20),\n" +
                "                sex VARCHAR(10)\n" +
                "                )WITH(\n" +
                "                'connector' = 'starrocks',\n" +
                "                'jdbc-url'='jdbc:mysql://node1:9039',\n" +
                "                'load-url'='node1:8039',\n" +
                "                'username'='root',\n" +
                "                'password'='',\n" +
                "                'table-name'='flink_student', \n" +
                "                'database-name'='flink',\n" +
                "                'sink.properties.format'='json',\n" +
                "                'sink.properties.strip_outer_array'='true' \n" +
                "                )");
        StatementSet statementSet = env.createStatementSet();
        statementSet.addInsertSql("insert into flink_student values(1001,'李四',19,'111.qq.com','女')," +
                "(1002,'张三',20,'222@qq.com','男')");
        statementSet.execute();
    }
}

class Student {
    private int uid;
    private String name;
    private int age;
    private String email;
    private String sex;

    public Student(int uid, String name, int age, String email, String sex) {
        this.uid = uid;
        this.name = name;
        this.age = age;
        this.email = email;
        this.sex = sex;
    }
}
