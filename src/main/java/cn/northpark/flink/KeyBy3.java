package cn.northpark.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy实例3 keyBy多个字段进行分组
 * @author bruce
 */
public class KeyBy3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //直接输入单词
        DataStreamSource<String> lines = env.socketTextStream("localhost",  4000);

        //辽宁,沈阳,1000
        //山东,青岛,2000
        //山东，青岛,2000
        //山东,烟台,1000
        SingleOutputStreamOperator<Tuple3<String, String, Double>> proCityAndMoney = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {

            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] words = value.split(",");
                String province = words[0];
                String city = words[1];
                Double money = Double.parseDouble(words[2]);
                return Tuple3.of(province, city, money);
            }
        });

        proCityAndMoney.keyBy(0,1).sum(2).print();


        env.execute("KeyBy3");

    }


}
