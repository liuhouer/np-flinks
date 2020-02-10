package cn.northpark.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author zhangyang
 *	使用Java API来开发Flink的批处理应用程序.
 */
public class BatchWC {

	public static void main(String[] args) throws Exception {

		//1.环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//2.read
		DataSource<String> readTextFile = env.readTextFile("D:\\mac.txt");
		
		readTextFile.print();
		
		//3.transform
		readTextFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
				// TODO Auto-generated method stub
				String[] tokens = value.toLowerCase().split("，");
				for (String token : tokens) {
					if(token.length()>0) {
						collector.collect(new Tuple2<String, Integer>(token,1));//每个单词数量设置为1，后面再统计/累加...
					}
				}
			}
		}).groupBy(0).sum(1).print();

		//4.execute
//		env.execute();
		
	}

}
