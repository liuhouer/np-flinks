package cn.northpark.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bruce
 * 揭秘subTask的编号
 */
public class PrintSink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 4000);

        source.print("the res is ").setParallelism(2);

        env.execute("PrintSink");


    }
}
