package cn.northpark.flink;

import org.apache.flink.api.common.functions.FilterFunction;
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
public class StreamingWordCountChain {

	public static void main(String[] args) throws Exception {
		 // step1 ：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // step2：读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 4000);


        // 拆词
        SingleOutputStreamOperator<String> words = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //过滤
        SingleOutputStreamOperator<String> filtered = words.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("a");
            }
        }).disableChaining();//将这个算子单独划分，生成一个Task，和其他的算子不再有operator chain
                //.startNewChain();//将这个开始划分，生成一个新的Task

        //把单词和1拼一块
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = filtered.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //分组、累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(0).sum(1);//.setParallelism(1);


        //sink
        sumed.print().setParallelism(2);

        //execute
        env.execute("StreamingWordCount");
	}
}
