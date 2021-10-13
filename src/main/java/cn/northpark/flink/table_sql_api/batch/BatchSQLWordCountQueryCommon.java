package cn.northpark.flink.table_sql_api.batch;

import cn.northpark.flink.WordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author bruce
 * 利用sql api进行离线查询
 * 从配置文件读取参数匹配
 * 适合不会编程的人员调用
 */
public class BatchSQLWordCountQueryCommon {
    public static void main(String[] args) throws Exception {

        ParameterTool parameters  = ParameterTool.fromPropertiesFile("/Users/bruce/Documents/workspace/np-flink/src/main/resources/sqlcfg.properties");

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
        tableEnv.registerDataSet(parameters.getRequired("table"),wordCountDataSource,parameters.getRequired("columns"));

        Table table = tableEnv.sqlQuery(parameters.getRequired("sql"));

        //把表转换成dataSet
        DataSet<Row> rowDataSet = tableEnv.toDataSet(table, Row.class);

        rowDataSet.print();

    }
}
