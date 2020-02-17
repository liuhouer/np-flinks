package cn.northpark.flink.project2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorState1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        env.setStateBackend(new FsStateBackend("file:////Users/bruce/Documents/workspace/np-flink/backEnd"));

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStreamSource<Tuple2<String, String>> lines = env.addSource(new NP_ExactlyOnceParallelismFileSource("/Users/bruce/Desktop/data"));

        lines.print();


        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 4000);

        socketTextStream.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String value) throws Exception {
                if(value.startsWith("jeyy")){
                    System.out.println(1/0);
                }
                return value;
            }
        });

        env.execute("OperatorState1");
    }
}
