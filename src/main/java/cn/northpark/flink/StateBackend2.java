package cn.northpark.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateBackend2 {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //只有开启了checkpoint 才会有重启策略
        env.enableCheckpointing(8000);

        //hdfs://localhost:9000/np-backend

        //设置重启策略为重启2次，间隔2秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,2));

        //设置StateBackend策略为本地文件系统
//        env.setStateBackend(new FsStateBackend("file:///Users/bruce/Documents/workspace/np-flink/np-stateBackend"));

        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/np-backend1"));

        //设置cancelJob或者异常退出Job以后不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 4000);

        SingleOutputStreamOperator<Tuple2<String, Integer>>  wordOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if (value.startsWith("jeyy")) {
                    throw new RuntimeException("jeyy来了，程序出错了！！！");
                }
                return Tuple2.of(value, 1);
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordOne.keyBy(0).sum(1);

        summed.print();

        env.execute("StateBackend2");


    }
}
