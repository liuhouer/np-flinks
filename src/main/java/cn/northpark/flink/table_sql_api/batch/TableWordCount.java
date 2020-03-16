package cn.northpark.flink.table_sql_api.batch;

import cn.northpark.flink.WordCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author bruce
 * 利用table api进行离线计算
 */
public class TableWordCount {
    public static void main(String[] args) throws Exception {

        //dataSet api
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //实时Table执行上下文
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        //模拟数据
        DataSource<WordCount> wordCountDataSource = env.fromElements(new WordCount("java", 1),
                new WordCount("scala", 1),
                new WordCount("java", 1),
                new WordCount("java", 1),
                new WordCount("flink", 1),
                new WordCount("flink", 1),
                new WordCount("vue", 1)


        );

        //将dataSet注册成表
        Table table = tableEnv.fromDataSet(wordCountDataSource);

        System.out.printf("schema---", table.getSchema());

        Table table2 = table
                .groupBy("word")
                .select("word, counts.sum as counts")
                .filter("counts >=2 ")
                .orderBy("counts.desc");

        //把表转换成dataSet
        DataSet<WordCount> rowDataSet = tableEnv.toDataSet(table2, WordCount.class);

        rowDataSet.print();

    }
}
