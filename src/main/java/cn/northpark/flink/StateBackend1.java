package cn.northpark.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhangyang
 * 重启策略
 */
public class StateBackend1 {

	public static void main(String[] args) throws Exception {
		 // step1 ：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //只有开启了checkpoint 才会有重启策略 默认是不重启
        env.enableCheckpointing(5000);//每隔5s进行一次checkpoint
        //默认的重启策略是无限重启  Integer.MAX_VALUE 次

        //重启重试次数
        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(3,2000));

        //设置状态存储的后端,一般写在flink的配置文件中
//        env.setStateBackend(new FsStateBackend("file:///Users/bruce/Documents/workspace/np-flink/np-backend"));
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/np-backend"));


        //程序异常退出或者人为cancel以后，不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // step2：读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 4000);


        //把单词和1拼一块
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = text.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if(value.startsWith("jeyy")){
                    throw new RuntimeException("jeyy来了，发生异常！！");
                }
                return Tuple2.of(value, 1);
            }
        });

        //分组、累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(0).sum(1);//.setParallelism(1);


        //sink
        sumed.print();

        //execute
        env.execute("StateBackend1");
	}
}
