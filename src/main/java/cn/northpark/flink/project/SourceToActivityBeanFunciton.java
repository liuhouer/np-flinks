package cn.northpark.flink.project;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *    u001,A1,2019-09-02 10:10:11,1 ,北京市
 *  * u002 ,A1,2019-09-0210:11:11,1,辽宁省
 *  * u001,A1,2019-09-02 10:11:11,2 ,北京市
 *  * u001,A1,2019-09-02 10:11:30,3, 北京市
 *  * u002 ,A1,2019-09-0210:12:11,2 ,辽宁省
 *  * u003,A2,2019-09-02 10:13:11,1, 山东省
 *  * u003 ,A22019-09-0210:13:20,2, 山东省
 *  * u003,A2 ,2019-09-0210:14:20,3, 山东省
 *  * u004,A1,2019-09-02 10:15:20,1, 北京市
 *  * u004, A1,2019-09-0210:15:20,2,北京市
 *  * u005,A1,2019-09-02 10:15:20, 1 ,河北省
 *  * u001,A2 2019-09-0210:16:11, 1 ,北京市
 *  * u001,A2, 2019-09-0210:16:11,2 ,北京市
 *  * u002 ,A1, 2019-09-0210:18:11,2,辽宁省
 *  * u002 ,A1,2019-09-02 10:19:11,3 ,辽宁省
 *  *
 *  *
 *  * id  name    last_update
 *  * A1  新人礼包 2019-10-15 11:36:36
 *  * A2  月末活动 2019-10-15 16:37:42
 *  * A3  周末活动 2019-10-15 11:44:23
 *  * A4  年度促销 2019-10-15 11:44:23
 */
public class SourceToActivityBeanFunciton extends RichMapFunction<String,ActivityBean> {


    private Connection connection = null;

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
