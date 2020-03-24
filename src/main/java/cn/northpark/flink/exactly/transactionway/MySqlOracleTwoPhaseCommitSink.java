package cn.northpark.flink.exactly.transactionway;

import cn.northpark.flink.util.HikariUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.io.serializer.avro.AvroSerialization;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * mysql | oracle 通过两阶段提交，实现消费的exactly-once(不多不少，仅消费一次)
 */
@Slf4j
public class MySqlOracleTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<Tuple3<String, String, String>, Connection, Void>  {

    private  String sourceType = "mysql";

    public MySqlOracleTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    public MySqlOracleTwoPhaseCommitSink(String sourceType) {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
        this.sourceType = sourceType;
    }

    @Override
    protected void invoke(Connection connection, Tuple3<String, String, String> objectNode, Context context) throws Exception {
        System.err.println("start invoke.......");
//        String value = objectNode.get("value").toString();
//        ActivityBean activityBean = JSON.parseObject(value, ActivityBean.class);

        log.info((connection==null)+"");
        String sql = "insert into t_word_counts values (?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, objectNode.f0);
        ps.setString(2, objectNode.f1);
        ps.setString(3, objectNode.f2);
        ps.executeUpdate();
        //手动制造异常
        //if(Integer.parseInt(value) == 15) System.out.println(1/0);

    }

    /**
     * 获取连接，开启手动提交事物
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        Connection connection = HikariUtils.getConnection(sourceType);
        log.info("start beginTransaction......."+connection);
        return connection;
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit......." + connection);
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        log.info("start commit......." + connection);
        HikariUtils.commit(connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        log.info("start abort rollback......." + connection);
        HikariUtils.rollback(connection);
    }

}

