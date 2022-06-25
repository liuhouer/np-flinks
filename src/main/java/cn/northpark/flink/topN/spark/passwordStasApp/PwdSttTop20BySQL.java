package cn.northpark.flink.topN.spark.passwordStasApp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * @author bruce
 * @date 2022年06月25日 12:40:03
 */
public class PwdSttTop20BySQL {

    private static final int topN = 20;
    public static void main(String[] args) throws AnalysisException {


        //创建JavaSparkContext
        SparkConf conf = new SparkConf();
        conf.setAppName("TopNJava")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        //1：首先获取两份数据中的核心字段
        JavaRDD<String> T1 = sc.textFile("C:\\Users\\admin\\Desktop\\2\\1.txt");
        JavaRDD<String> T2 = sc.textFile("C:\\Users\\admin\\Desktop\\2\\2.txt");
        JavaRDD<String> T3 = sc.textFile("C:\\Users\\admin\\Desktop\\2\\3.txt");
        JavaRDD<String> T4 = sc.textFile("C:\\Users\\admin\\Desktop\\2\\4.txt");

        //2.对数据进行切割，把一行数据切分成一个一个的单词
        //注意：FlatMapFunction的泛型，第一个参数表示输入数据类型，第二个表示是输出数据类型
        JavaRDD<String> wordsRDD = T1.union(T2).union(T3).union(T4).flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {

                // 去掉所有的键盘上的不可输入字符，不包括双字节的，32-126
                String pattern = "[^\040-\176]";
                String line_ = line.toString().replaceAll(pattern, " ");

                return Arrays.asList(line_.split(" ")).iterator();
            }
        });

        //3：迭代words，将每个word转换为(word,1)这种形式
        //注意：PairFunction的泛型，
        //第一个参数是输入数据类型
        //第二个是输出tuple中的第一个参数类型，第三个是输出tuple中的第二个参数类型
        //注意：如果后面需要使用到....ByKey，前面都需要使用mapToPair去处理
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //4：根据key(其实就是word)进行分组聚合统计
        JavaPairRDD<String, Integer> wordCountRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        //5，转为row
        JavaRDD<Row> rowJavaRDD = wordCountRDD.map(new Function<Tuple2<String, Integer>, Row>() {
            public Row call(Tuple2<String, Integer> line) throws Exception {
                return RowFactory.create(line._1, line._2);
            }
        });


        /**
         * 6-动态构造DataFrame的元数据。
         */
        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("pwd",DataTypes.StringType ,true));
        structFields.add(DataTypes.createStructField("count",DataTypes.IntegerType,true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowJavaRDD, structType);

        dataFrame.createTempView("t_pwd_count");


        //取前20
        Dataset<Row> table = sqlContext.sql("select * from t_pwd_count order by count desc limit  " + topN);

        //打印
        table.show();

        //写入mysql

        //数据库内容
        String url = "jdbc:mysql://localhost:3306/test";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","123456");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        /**
         * 第四步：将数据写入到person表中
         */
        table.write().mode("append").jdbc(url,"t_pwd_count",connectionProperties);


        sc.stop();
    }



}
