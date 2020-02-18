package cn.northpark.flink.project3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        env.setStateBackend(new FsStateBackend("file:///Users/bruce/Documents/workspace/np-flink/backEnd"));

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));

        env.addSource(new MyExactlyOnceParaFileSource("/Users/bruce/Desktop/data")).print();

        //THROW exception
        env.socketTextStream("localhost",4000).map(new MapFunction<String, Object>() {
            @Override
            public Object map(String value) throws Exception {
                if(value.startsWith("jeyy")){
                    System.out.println(1/0);
                }
                return value;
            }
        });

        env.execute("OperatorStateTest");
    }
}
