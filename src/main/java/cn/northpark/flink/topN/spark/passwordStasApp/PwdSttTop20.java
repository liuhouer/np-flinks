package cn.northpark.flink.topN.spark.passwordStasApp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author bruce
 * @date 2022年06月25日 12:40:03
 */
public class PwdSttTop20 {

    private static final int topN = 20;
    public static void main(String[] args) {


        //创建JavaSparkContext
        SparkConf conf = new SparkConf();
        conf.setAppName("TopNJava")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

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

        //5.换位置
        JavaPairRDD<Integer, String> countAndWord = wordCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });


        //6.取出topN: sort and take
        List<Tuple2<Integer, String>> top20 = countAndWord.sortByKey(false).take(topN);

        //打印
        top20.forEach(item->{
            System.out.print("pwd = "+item._2 +"||"+"count = "+item._1);
            System.out.println();
        });


        sc.close();
    }



}
