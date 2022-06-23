package cn.northpark.flink.topN.spark;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * @author bruce
 * @date 2022年06月24日 07:35:50
 */
public class TopNJava {

    public static void main(String[] args) {
        //创建JavaSparkContext
        SparkConf conf = new SparkConf();
        conf.setAppName("TopNJava")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //1：首先获取两份数据中的核心字段，使用fastjson包解析数据
        JavaRDD<String> videoInfoRDD = sc.textFile("D:\\video_info.txt");
        JavaRDD<String> giftRecordRDD = sc.textFile("D:\\gift_record.txt");
        //(vid,(uid,area))
        JavaPairRDD<String, Tuple2<String, String>> videoInfoFieldRDD = videoInfoRDD.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String line)
                    throws Exception {
                JSONObject jsonObj = JSON.parseObject(line);
                String vid = jsonObj.getString("vid");
                String uid = jsonObj.getString("uid");
                String area = jsonObj.getString("area");
                return new Tuple2<String, Tuple2<String, String>>(vid, new Tuple2<String, String>(uid, area));
            }
        });

        //(vid,gold)
        JavaPairRDD<String, Integer> giftRecordFieldRDD = giftRecordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                JSONObject jsonObj = JSON.parseObject(line);
                String vid = jsonObj.getString("vid");
                Integer gold = Integer.parseInt(jsonObj.getString("gold"));
                return new Tuple2<String, Integer>(vid, gold);
            }
        });

        //2：对用户送礼记录数据进行聚合，对相同vid的数据求和
        //(vid,gold_sum)
        JavaPairRDD<String, Integer> giftRecordFieldAggRDD = giftRecordFieldRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        //3：把这两份数据join到一块，vid作为join的key
        //(vid,((uid,area),gold_sum))
        //4：使用map迭代join之后的数据，最后获取到uid、area、gold_sum字段
        //((uid,area),gold_sum)
        JavaPairRDD<Tuple2<String, String>, Integer> joinMapRDD = videoInfoFieldRDD.join(giftRecordFieldAggRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, Integer>>, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, Tuple2<Tuple2<String, String>, Integer>> tup)
                    throws Exception {
                //joinRDD:(vid,((uid,area),gold_sum))
                //获取uid
                String uid = tup._2._1._1;
                //获取area
                String area = tup._2._1._2;
                //获取gold_sum
                Integer gold_sum = tup._2._2;
                return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(uid, area), gold_sum);
            }
        });
        //5：使用reduceByKey算子对数据进行聚合
        //((uid,area),gold_sum_all)
        JavaPairRDD<Tuple2<String, String>, Integer> reduceRDD = joinMapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        //6：接下来对需要使用groupByKey对数据进行分组，所以先使用map进行转换
        //map：(area,(uid,gold_sum_all))
        //groupByKey: area,<(uid,gold_sum_all),(uid,gold_sum_all),(uid,gold_sum_all)>
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = reduceRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> tup)
                    throws Exception {
                return new Tuple2<String, Tuple2<String, Integer>>(tup._1._2, new Tuple2<String, Integer>(tup._1._1, tup._2));
            }
        }).groupByKey();

        //7：使用map迭代每个分组内的数据，按照金币数量倒序排序，取前N个，最终输出area,topN
        //(area,topN)
        JavaRDD<Tuple2<String, String>> top3RDD = groupRDD.map(new Function<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> tup)
                    throws Exception {
                String area = tup._1;
                ArrayList<Tuple2<String, Integer>> tupleList = Lists.newArrayList(tup._2);
                //对集合中的元素排序
                Collections.sort(tupleList, new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
                        return t2._2 - t1._2;
                    }
                });
                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < tupleList.size(); i++) {
                    if (i < 3) {//top 3
                        Tuple2<String, Integer> t = tupleList.get(i);
                        if (i != 0) {
                            sb.append(",");
                        }
                        sb.append(t._1 + ":" + t._2);
                    }
                }
                return new Tuple2<String, String>(area, sb.toString());
            }
        });

        //8：使用foreach将结果打印到控制台，多个字段使用制表符分割
        //area	topN
        top3RDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tup) throws Exception {
                System.out.println(tup._1+"\t"+tup._2);
            }
        });

        sc.stop();
    }
}
