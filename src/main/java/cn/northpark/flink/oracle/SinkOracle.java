package cn.northpark.flink.oracle;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

@Slf4j
public class SinkOracle extends RichSinkFunction<Tuple3<String, String, String>> {

    private Connection connection;
    private PreparedStatement statement;

    // 1,初始化
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("oracle.jdbc.OracleDriver");
        connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:test", "root", "123456");
    }

    // 2,执行
    @Override
    public void invoke(Tuple3<String, String, String> objectNode, Context context) throws Exception {
        log.info("---------------------->", connection);
        String sql = "INSERT INTO \"FLINK\".\"t_word_counts\"(\"id\", \"word\", \"uptime\")  values (?,?,?) " ;
        PreparedStatement ps = connection.prepareStatement(sql);
        log.info("sql--------------------->", sql);
        ps.setString(1, objectNode.f0);
        ps.setString(2, objectNode.f1);
        ps.setString(3, objectNode.f2);
        ps.executeUpdate();
    }

    // 3,关闭
    @Override
    public void close() throws Exception {
        super.close();
        if (statement != null)
            statement.close();
        if (connection != null)
            connection.close();
    }
}
