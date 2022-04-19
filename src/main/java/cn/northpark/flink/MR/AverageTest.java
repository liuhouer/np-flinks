package cn.northpark.flink.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.net.URI;

/**
 * @author bruce
 * @date 2022年04月19日 16:36:33
 *
 * 小民	语文	80
 * 小民	数学	98
 * 小民	英语	89
 * 小芳	语文	88
 * 小芳	数学	99
 * 小芳	英语	90
 */
public class AverageTest {
    // 定义输入路径
    private static final String INPUT_PATH = "hdfs://node1:9000/BigDataProject/车载数据.csv";
    // 定义输出路径
    private static final String OUT_PATH = "hdfs://node1:9000/BigDataProject/A";

    public static void main(String[] args) {

        try {
            // 创建配置信息
            Configuration conf = new Configuration();

            // 创建文件系统
            FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf);
            // 如果输出目录存在，我们就删除
            if (fileSystem.exists(new Path(OUT_PATH))) {
                fileSystem.delete(new Path(OUT_PATH), true);
            }

            // 创建任务
            Job job = new Job(conf, AverageTest.class.getName());

            //1.1	设置输入目录和设置输入数据格式化的类
            FileInputFormat.setInputPaths(job, INPUT_PATH);
            job.setInputFormatClass(TextInputFormat.class);

            //1.2	设置自定义Mapper类和设置map函数输出数据的key和value的类型
            job.setMapperClass(AverageMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //1.3	设置分区和reduce数量(reduce的数量，和分区的数量对应，因为分区为一个，所以reduce的数量也是一个)
            job.setPartitionerClass(HashPartitioner.class);
            job.setNumReduceTasks(1);

            //1.4	排序
            //1.5	归约
            //2.1	Shuffle把数据从Map端拷贝到Reduce端。
            //2.2	指定Reducer类和输出key和value的类型
            job.setReducerClass(AverageReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            //2.3	指定输出的路径和设置输出的格式化类
            FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
            job.setOutputFormatClass(TextOutputFormat.class);


            // 提交作业 退出
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text>{
        //设置输出的key和value
        private Text outKey = new Text();
        private Text outValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            //获取输入的行
            String line = value.toString();
            //取出无效记录
            if (line == null || line.equals("")){
                return ;
            }
            //对数据进行切分
            String[] splits = line.split("\t");

            //截取姓名和成绩
            String name = splits[0];
            String score = splits[2];
            //设置输出的Key和value
            outKey.set(name);
            outValue.set(score);
            //将结果写出去
            context.write(outKey, outValue);

        }

    }

    public static class AverageReducer extends Reducer<Text, Text, Text, FloatWritable>{
        //定义写出去的Key和value
        private Text name = new Text();
        private FloatWritable avg = new FloatWritable();
        @Override
        protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            //定义科目数量
            int courseCount = 0;
            //定义中成绩
            int sum = 0;
            //定义平均分
            float average = 0;

            //遍历集合求总成绩
            for (Text val : value){
                sum += Integer.parseInt(val.toString());
                courseCount ++;
            }

            //求平均成绩
            average = sum / courseCount;

            //设置写出去的名字和成绩
            name.set(key);
            avg.set(average);

            //把结果写出去
            context.write(name, avg);
        }
    }

}
