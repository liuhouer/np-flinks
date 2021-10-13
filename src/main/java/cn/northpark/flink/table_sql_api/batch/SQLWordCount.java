package cn.northpark.flink.table_sql_api.batch;

import cn.northpark.flink.WordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

/**
 * @author bruce
 * 利用sql api进行离线计算
 */
public class SQLWordCount {
    public static void main(String[] args) throws Exception {

        //dataSet api
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //实时Table执行上下文
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        //模拟数据
        DataSource<WordCount> wordCountDataSource = env.fromElements(
                new WordCount("java", 1),
                new WordCount("scala", 1),
                new WordCount("java", 1),
                new WordCount("java", 1),
                new WordCount("flink", 1),
                new WordCount("flink", 1),
                new WordCount("vue", 1)


        );


        //将dataSet注册成表,指定字段名称
        tableEnv.registerDataSet("word_count",wordCountDataSource,"word,counts");

        String sql = "select word,sum(counts) as counts from word_count group by word having sum(counts) >=2 order by counts desc ";

        Table table = tableEnv.sqlQuery(sql);

        //把表转换成dataSet
        DataSet<WordCount> rowDataSet = tableEnv.toDataSet(table, WordCount.class);

        rowDataSet.print();

    }
}
