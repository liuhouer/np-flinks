package cn.northpark.flink;


import cn.northpark.flink.bean.StatisticsVO;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Stat;
import sun.rmi.runtime.Log;

/**
 * @author bruce
 *	NorthPark多维度分析统计请求日志
 */
@Slf4j
public class NorthParkSTT {

	public static void main(String[] args) throws Exception {

		//1.环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//2.read
		DataSource<String> readTextFile = env.readTextFile("C:\\Users\\Bruce\\Desktop\\STT.log");
		

		//3.transform
		FilterOperator<StatisticsVO> map = readTextFile.map(new MapFunction<String, StatisticsVO>() {

			@Override
			public StatisticsVO map(String value) throws Exception {
				int start_index = value.indexOf("[Statistics Info]^");
				String replace_1 = value.substring(start_index).replace("[Statistics Info]^", "");


//				JSONObject jsonObject = JSON.parseObject(sub_string);

				StatisticsVO vo = JSON.parseObject(replace_1, StatisticsVO.class);

				return vo;
			}

		}).filter(new FilterFunction<StatisticsVO>() {
			@Override
			public boolean filter(StatisticsVO value) throws Exception {
				return value.url != null;
			}
		});

		//统计url
//		map.flatMap(new FlatMapFunction<StatisticsVO, Tuple2<String,Integer>>() {
//			@Override
//			public void flatMap(StatisticsVO value, Collector<Tuple2<String, Integer>> out) throws Exception {
//				out.collect(Tuple2.of(value.url, 1));
//			}
//		}).groupBy(0).sum(1).print();

		//统计用户
		map.filter(new FilterFunction<StatisticsVO>() {
			@Override
			public boolean filter(StatisticsVO vo) throws Exception {

				return vo.userVO != null;
			}
		}).flatMap(new FlatMapFunction<StatisticsVO, Tuple2<String,Integer>>() {
			@Override
			public void flatMap(StatisticsVO value, Collector<Tuple2<String, Integer>> out) throws Exception {
				out.collect(Tuple2.of(value.userVO.username, 1));
			}
		}).groupBy(0).sum(1).print();

		//统计用户+请求页面次数
//		map.filter(new FilterFunction<StatisticsVO>() {
//			@Override
//			public boolean filter(StatisticsVO vo) throws Exception {
//
//				return vo.userVO != null;
//			}
//		}).flatMap(new FlatMapFunction<StatisticsVO, Tuple2<String,Integer>>() {
//			@Override
//			public void flatMap(StatisticsVO value, Collector<Tuple2<String, Integer>> out) throws Exception {
//				out.collect(Tuple2.of(value.userVO.username+"_"+value.url, 1));
//			}
//		}).groupBy(0).sum(1).print();

//		map.groupBy("url").sum(0).print();
		//4.execute
//		env.execute();
		
	}

}
