package cn.northpark.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy实例一
 * @author bruce
 */
public class KeyBy1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //直接输入单词
        DataStreamSource<String> lines = env.socketTextStream("localhost",  4000);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = lines.map(i -> Tuple2.of(i, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));


        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = map.keyBy(0);

        keyed.print();

        env.execute("KeyBy1");

    }


}
