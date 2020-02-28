package cn.northpark.flink.exactly.transactionway;

import cn.northpark.flink.project.ActivityBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 通过两阶段提交，实现消费的exactly-once(不多不少，仅消费一次)
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<ObjectNode, Connection, Void> {

    public MySqlTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(Connection connection, ObjectNode objectNode, Context context) throws Exception {
        System.err.println("start invoke.......");
        String stu = objectNode.get("value").toString();
        ActivityBean activityBean = JSON.parseObject(stu, ActivityBean.class);
        String sql = "insert into T_ACTIVITY values (?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, activityBean.aid);
        ps.setString(2, activityBean.activityName);
        ps.setString(3, activityBean.time);
        ps.executeUpdate();
        //手动制造异常
        //if(Integer.parseInt(value) == 15) System.out.println(1/0);

    }

    @Override
    protected Connection beginTransaction() throws Exception {
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "123456");
        System.err.println("start beginTransaction......."+connection);
        return connection;
    }

    @Override
    protected void preCommit(Connection connection) throws Exception {
        System.err.println("start preCommit......." + connection);
    }

    @Override
    protected void commit(Connection connection) {
        System.err.println("start commit......." + connection);
        try {
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void abort(Connection connection) {
        System.err.println("start abort rollback......." + connection);
        try {
            connection.rollback();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

