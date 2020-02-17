package cn.northpark.flink.project.syncIO.function;

import cn.northpark.flink.project.ActivityBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class NP_MySqlSinkFunction extends RichSinkFunction<ActivityBean> {

    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink?characterEncoding=UTF-8","root","123456");

    }

    @Override
    public void invoke(ActivityBean bean, Context context) throws Exception {

        //插入或者更新
        //INSERT INTO t_activity_counts (id, event, counts) VALUES (?, ?, ?)
        // ON DUPLICATE KEY UPDATE counts = ?

        PreparedStatement preparedStatement = null;
        try{

            preparedStatement = connection.prepareStatement(" INSERT INTO t_activity_counts (id, event, counts) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE counts = ? ");
            preparedStatement.setString(1,bean.aid);
            preparedStatement.setInt(2,bean.eventType);
            preparedStatement.setInt(3,bean.counts);
            preparedStatement.setInt(4,bean.counts);

            preparedStatement.executeUpdate();
        }finally {
            if(preparedStatement!=null){
                preparedStatement.close();
            }
        }



    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
