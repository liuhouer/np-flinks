package cn.northpark.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhangyang
 * 按照步骤来一步步拆分Task是如何划分的
 * wc统计的数据我们源自于socket
 */
public class StreamingWordCount2 {

	public static void main(String[] args) throws Exception {
		 // step1 ：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // step2：读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 4000);

        env.setParallelism(2);

        // 拆词 + 拼数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });


        //分组、累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(0).sum(1);


        //sink
        sumed.print().setParallelism(2);

        //execute
        env.execute("StreamingWordCount");
	}
}
