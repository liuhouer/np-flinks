package cn.northpark.flink.exactly.transactionway;

import cn.northpark.flink.util.FlinkUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.UUID;

/***
 * Flink从kafka读取数据写入mysql 并且实现exactly once
 * @author bruce
 */
public class FlinkKafkaToMysql {

    public static void main(String[] args) throws  Exception{

//        ParameterTool parameters = ParameterTool.fromArgs(args);

        InputStream is = FlinkKafkaToMysql.class.getClassLoader().getResourceAsStream("config.properties");

//        ParameterTool parameters  = ParameterTool.fromPropertiesFile("/Users/bruce/Documents/workspace/np-flink/src/main/resources/config.properties");

        ParameterTool parameters  = ParameterTool.fromPropertiesFile(is);

        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);


        SingleOutputStreamOperator<Tuple3<String, String, String>> words = kafkaStream.flatMap(new FlatMapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, String>> out) throws Exception {
                    if(!StringUtils.isNullOrWhitespaceOnly(value)){

                        out.collect(Tuple3.of(UUID.randomUUID().toString(),value, LocalDateTime.now().toString()));
                    }
            }
        });

        words.print();

        words.addSink(new MySqlTwoPhaseCommitSink());


        FlinkUtils.getEnv().execute("FlinkKafkaToMysql");

    }
}
