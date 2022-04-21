package cn.northpark.flink.oracle;

import cn.northpark.flink.exactly.transactionway.FlinkKafkaToMysql;
import cn.northpark.flink.util.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.InputStream;

/***
 * flink 读取kafka数据结合Oracle查询 整合数据
 * @author bruce
 */
public class FlinkKafkaLinkOracleSource {

    public static void main(String[] args) throws  Exception{

        InputStream is = FlinkKafkaToMysql.class.getClassLoader().getResourceAsStream("config.properties");

        ParameterTool parameters = ParameterTool.fromPropertiesFile(is);

        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);


        SingleOutputStreamOperator<Tuple3<String, String, String>> tupleData = kafkaStream.map(new OracleToTupleFunciton());

        tupleData.print();

        FlinkUtils.getEnv().execute("FlinkKafkaLinkOracleSource");

    }
}
