package cn.northpark.flink.exactly.transactionway;

import cn.northpark.flink.util.HikariUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * mysql通过两阶段提交，实现消费的exactly-once(不多不少，仅消费一次)
 */
@Slf4j
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<Tuple3<String, String, String>, MySqlTwoPhaseCommitSink.ConnectionState, Void> {


    public MySqlTwoPhaseCommitSink() {

        super(new KryoSerializer<>(MySqlTwoPhaseCommitSink.ConnectionState.class, new ExecutionConfig()), VoidSerializer.INSTANCE);

    }


    @Override
    protected void invoke(MySqlTwoPhaseCommitSink.ConnectionState connectionState, Tuple3<String, String, String> objectNode, Context context) throws Exception {
        System.err.println("start invoke.......");
//        String value = objectNode.get("value").toString();
//        ActivityBean activityBean = JSON.parseObject(value, ActivityBean.class);
        Connection connection = connectionState.connection;
        log.info("---------------------->", connection);
        String sql = "insert into t_word_counts values (?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        log.info("sql--------------------->", sql);
        ps.setString(1, objectNode.f0);
        ps.setString(2, objectNode.f1);
        ps.setString(3, objectNode.f2);
        ps.executeUpdate();


    }

    /**
     * 获取连接，开启手动提交事物
     *
     * @return
     * @throws Exception
     */
    @Override
    protected MySqlTwoPhaseCommitSink.ConnectionState beginTransaction() throws Exception {

        Connection connection = HikariUtils.getConnection();

        log.info("start beginTransaction......." + connection);

        return new ConnectionState(connection);
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     *
     * @param connectionState
     * @throws Exception
     */
    @Override
    protected void preCommit(MySqlTwoPhaseCommitSink.ConnectionState connectionState) throws Exception {
        log.info("start preCommit......." + connectionState);
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     *
     * @param connectionState
     */
    @Override
    protected void commit(MySqlTwoPhaseCommitSink.ConnectionState connectionState) {
        log.info("start commit......." + connectionState);

        Connection connection = connectionState.connection;

        try {
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("提交事物异常");
        }
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     *
     * @param connectionState
     */
    @Override
    protected void abort(MySqlTwoPhaseCommitSink.ConnectionState connectionState) {
        log.info("start abort rollback......." + connectionState);
        Connection connection = connectionState.connection;
        try {
            connection.rollback();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("回滚事物异常");
        }
    }

    static class ConnectionState {

        private final transient Connection connection;

        ConnectionState(Connection connection) {

            this.connection = connection;
        }

    }

}

