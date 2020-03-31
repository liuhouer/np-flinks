package cn.northpark.flink.oracle;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *
 */
public class OracleToTupleFunciton extends RichMapFunction<String, Tuple3<String, String, String>> {


    private transient Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("oracle.jdbc.OracleDriver");
        connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:test", "root", "123456");
    }


    @Override
    public Tuple3<String, String, String> map(String word) throws Exception {

        PreparedStatement preparedStatement = connection.prepareStatement("select \"id\",\"word\",\"uptime\" from FLINK.\"t_word_counts\" WHERE \"word\" = ?");

        preparedStatement.setString(1, word);
        ResultSet resultSet = preparedStatement.executeQuery();
        String id = "";
        String uptime = "";
        while (resultSet.next()) {
            id = resultSet.getString(1);
            uptime = resultSet.getString(3);
        }

        preparedStatement.close();


        return Tuple3.of(id, word, uptime);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }

}
