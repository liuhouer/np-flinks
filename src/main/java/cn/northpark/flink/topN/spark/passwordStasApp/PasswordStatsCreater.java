package cn.northpark.flink.topN.spark.passwordStasApp;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PasswordStatsCreater {
	public static class PWDMapper extends Mapper<Object, Text, Text, IntWritable> {

		final static IntWritable one = new IntWritable(1);
		Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 除了0-9, a-z, A-Z以外的字符
			// 去掉所有的键盘上的不可输入字符，不包括双字节的，32-126
			String pattern = "[^\040-\176]";
			String line = value.toString().replaceAll(pattern, " ");
			StringTokenizer itr = new StringTokenizer(line);

			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class PWDSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// String[] otherArgs;
		try {

			String srcPath[] = { "/user/tian/pwdfiles/1.txt", "/user/tian/pwdfiles/2.txt", "/user/tian/pwdfiles/3.txt",
					"/user/tian/pwdfiles/4.txt" };
			String outPath = "/user/tian/out";
			Path tempDir = new Path("wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
			Job job = Job.getInstance(conf, "password stats");

			job.setJarByClass(PasswordStatsCreater.class);
			job.setMapperClass(PWDMapper.class);
			job.setCombinerClass(PWDSumReducer.class);
			job.setReducerClass(PWDSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			for (int i = 0; i < srcPath.length - 1; i++) {
				FileInputFormat.addInputPath(job, new Path(srcPath[i]));
			}
			FileOutputFormat.setOutputPath(job, tempDir);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (job.waitForCompletion(true)) {
				Job sortJob = Job.getInstance(conf, "sort");
				sortJob.setJarByClass(PasswordStatsCreater.class);

				FileInputFormat.addInputPath(sortJob, tempDir);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);
				sortJob.setMapperClass(InverseMapper.class);
				sortJob.setNumReduceTasks(1);
				FileOutputFormat.setOutputPath(sortJob, new Path(outPath));
				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
				// sortJob.setSortComparatorClass(IntWritable.Comparator.class);
				FileOutputFormat.setOutputPath(job, new Path(outPath));
				System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
			}
			FileSystem.get(conf).deleteOnExit(tempDir);

		} catch (IOException | ClassNotFoundException | IllegalStateException | IllegalArgumentException
				| InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
