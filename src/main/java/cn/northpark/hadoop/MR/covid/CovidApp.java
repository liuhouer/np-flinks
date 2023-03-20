package cn.northpark.hadoop.MR.covid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 *
 *  //hadoop提交作业
 *  1.hadoop jar np_hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop.CovidApp
 *
 *  //列出输出文件列表
 *  2.hdfs dfs -ls /BigDataProject/A
 *
 * //查看输出结果
 *  3.hadoop fs -cat /BigDataProject/A/part-r-00000
 * @author bruce
 * @date 2023年03月20日 14:00:35
 */
public class CovidApp {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //本地执行
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "Covid");
        job.setJarByClass(CovidApp.class);
        job.setMapperClass(CovidMapper.class);
        job.setReducerClass(CovidReducer.class);

        //map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Covid.class);

        //out
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("C:\\Users\\Bruce\\Downloads\\COVID-19.dat"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Bruce\\Downloads\\output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
