package cn.northpark.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy实例2
 * @author bruce
 */
public class KeyBy2Bean {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //直接输入单词
        DataStreamSource<String> lines = env.socketTextStream("localhost",  4000);

        SingleOutputStreamOperator<WordCount> map = lines.map(new MapFunction<String, WordCount>() {

            @Override
            public WordCount map(String value) throws Exception {
                return WordCount.of(value, 1);
            }
        });

        KeyedStream<WordCount, Tuple> word = map.keyBy("word");

        word.print();

        env.execute("KeyBy2Bean");

    }


}
