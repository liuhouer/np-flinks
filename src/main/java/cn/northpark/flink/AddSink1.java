package cn.northpark.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author bruce
 * 通过自定义sink来讲解subTask的编号问题
 *
 */
public class AddSink1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 4000);


        SingleOutputStreamOperator<Tuple2<String, Long>> maped = socketTextStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });

//        maped.print("the result is ").setParallelism(2);

        maped.addSink(new RichSinkFunction<Tuple2<String, Long>>() {

            @Override
            public void invoke(Tuple2<String, Long> value, Context context) throws Exception {

                System.out.println(getRuntimeContext().getIndexOfThisSubtask() +1 + "> "+value);

            }
        });


        env.execute("AddSink1");

    }
}
