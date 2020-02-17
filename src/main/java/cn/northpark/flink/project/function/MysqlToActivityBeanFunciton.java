package cn.northpark.flink.project.function;


import cn.northpark.flink.project.ActivityBean;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 */
public class MysqlToActivityBeanFunciton extends RichMapFunction<String, ActivityBean> {


    private transient Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink?characterEncoding=UTF-8","root","123456");
    }


    @Override
    public ActivityBean map(String line) throws Exception {
        String[] fields = line.split( ",");
        //u001,A1,2019-09-02 10:10:11,1 ,北京市

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT NAME FROM T_ACTIVITY  WHERE ID = ?");

        String uid = fields[0] ;
        String aid = fields[1] ;
        String time = fields[2] ;
        int eventType = Integer.parseInt(fields[3]) ;
        String province = fields[4] ;
        String activityName = null;
        preparedStatement.setString(1,aid);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            activityName = resultSet.getString(1);
        }

        preparedStatement.close();


        return ActivityBean.of(uid,aid,activityName,time,eventType,province);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }

}
