package cn.northpark.flink.oracle;

import cn.northpark.flink.exactly.transactionway.FlinkKafkaToMysql;
import cn.northpark.flink.oracle.SinkOracle;
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
 * @author bruce
 */
public class FlinkKafkaSink {

    public static void main(String[] args) throws  Exception{

        InputStream is = FlinkKafkaToMysql.class.getClassLoader().getResourceAsStream("config.properties");

        ParameterTool parameters = ParameterTool.fromPropertiesFile(is);

        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);


        SingleOutputStreamOperator<Tuple3<String, String, String>> words = kafkaStream.flatMap(new FlatMapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, String>> out) throws Exception {
                if (!StringUtils.isNullOrWhitespaceOnly(value)) {

                    out.collect(Tuple3.of(UUID.randomUUID().toString(), value, LocalDateTime.now().toString()));
                }
            }
        });

        words.print();

        words.addSink(new SinkOracle());

        FlinkUtils.getEnv().execute("FlinkKafkaPrint");

    }
}
