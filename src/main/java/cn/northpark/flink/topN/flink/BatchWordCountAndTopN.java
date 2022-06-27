package cn.northpark.flink.topN.flink;

import cn.northpark.flink.WordCount;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author bruce
 * 利用sql api进行离线计算
 */
public class BatchWordCountAndTopN {
    public static void main(String[] args) throws Exception {

        //dataSet api
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //实时Table执行上下文
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        //读取数据
        DataSource<String> wordCountDataSource1 = env.readTextFile("C:\\Users\\Bruce\\Desktop\\2\\1.txt");
        DataSource<String> wordCountDataSource2 = env.readTextFile("C:\\Users\\Bruce\\Desktop\\2\\2.txt");
        DataSource<String> wordCountDataSource3 = env.readTextFile("C:\\Users\\Bruce\\Desktop\\2\\3.txt");
        DataSource<String> wordCountDataSource4 = env.readTextFile("C:\\Users\\Bruce\\Desktop\\2\\4.txt");


        //把4个文件流组装起来
        UnionOperator<String> unionSource = wordCountDataSource1.union(wordCountDataSource2).union(wordCountDataSource3).union(wordCountDataSource4);


        //拆分口令
        FlatMapOperator<String, String> tokens = unionSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split("\t");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //把口令和1组装,并且过滤空值
        MapOperator<String, Tuple2<String, Integer>> tokenAndOne = tokens.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return StringUtils.isNotBlank(value);
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //分组、累加
        AggregateOperator<Tuple2<String, Integer>> summed = tokenAndOne.groupBy(0).sum(1);

        //将dataSet注册成表,指定字段名称
        tableEnv.registerDataSet("word_count",summed,"word,counts");


        //查询top N
        String sql = "select word,counts from word_count order by counts desc limit 20";

        Table table = tableEnv.sqlQuery(sql);


        //https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/jdbc/
        //-- register a MySQL table 'users' in Flink SQL
        //CREATE TABLE MyUserTable (
        //  id BIGINT,
        //  name STRING,
        //  age INT,
        //  status BOOLEAN,
        //  PRIMARY KEY (id) NOT ENFORCED
        //) WITH (
        //   'connector' = 'jdbc',
        //   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
        //   'table-name' = 'users'
        //);

        //和mysql库建立连接
//        String t_word_count = " create table t_word_count " +
//                " ( " +
//                "  word                STRING , " +
//                "  counts                 INTEGER  " +
//                //"  PRIMARY KEY(word)  NOT ENFORCED " + //设置入库主键以后，自动适配insert和update
//
//                " ) WITH (" +
//
//                "   'connector' = 'jdbc'," +
//                "   'url' = 'jdbc:mysql://localhost:3306/flink'," +
//                "   'table-name' = 't_word_count'," +
//                "   'username' = 'root'," +
//                "   'password' = '123456'" +
//
//                " )";
//
//
//        tableEnv.executeSql(t_word_count);
//
//
//        //把统计的结果写入mysql
//        String insertSQL ="INSERT INTO t_word_count select  word, counts FROM t_word_count";
//
//
//        tableEnv.executeSql(insertSQL);

        //把表转换成dataSet
        DataSet<WordCount> rowDataSet = tableEnv.toDataSet(table, WordCount.class);


        rowDataSet.print();

        //结果写到txt
        rowDataSet.writeAsText("C:/Users/Bruce/Desktop/top20.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("SQLWordCount");

    }
}
