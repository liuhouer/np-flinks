package cn.northpark.flink.MR;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * 需求：计算每天的加速度平均值和速度平均值
 *
 *日期	                加速度幅值	速度	仪器编号
 * 20210512 09:49:00	0.100000001	189	58381
 * 20210603 09:45:00	0.100000001	189	58351
 * 20210613 09:28:00	0.100000001	188	58351
 * 20210912 05:05:00	0.100000001	184	58270
 * 20210914 05:04:00	0.100000001	186	58270
 * 20210916 05:11:00	0.100000001	186	58270
 * 20210923 10:09:00	0.109999999	186	50190
 * 20211006 12:36:00	0.100000001	187	55431
 *
 *
 *  //hadoop提交作业
 *  1.hadoop jar np_hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop.CarSttDayJob
 *
 *  //列出输出文件列表
 *  2.hdfs dfs -ls /BigDataProject/B
 *
 * //查看输出结果
 *  3.hadoop fs -cat /BigDataProject/B/part-r-00000
 */
public class CarSttDayJob {

    // 定义输入路径
    private static final String INPUT_PATH = "/BigDataProject/车载数据.csv";
    // 定义输出路径
    private static final String OUT_PATH = "/BigDataProject/B";
    /**
     * Map阶段
     */
    public static class MyMapper extends Mapper<LongWritable, Text,Text, hadoop.CarBean>{
        Logger logger = LoggerFactory.getLogger(MyMapper.class);

        private Text outK = new Text();
        private hadoop.CarBean outV = new hadoop.CarBean();
        /**
         * 需要实现map函数
         * 这个map函数就是可以接收<k1,v1>，产生<k2，v2>
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {


            // 1获取一行转为String
            String line = value.toString();

            //2 按照逗号分割
            //日期	加速度幅值	速度	仪器编号
            //20210512 09:49:00	0.100000001	189	58381
            String[] csvComments = line.split(",");

            //3 获取需要的值
            String datetime = csvComments[0];
            String upSpeed = csvComments[1];
            String speed = csvComments[2];
            String no = csvComments[3];

            //4 封装到对象
            outV.setUpSpeed(upSpeed);
            outV.setSpeed(speed);
            outV.setNo(no);


            String date = "";
            if(StringUtils.isNoneBlank(datetime) && datetime.length()>8){
                date = datetime.substring(0,8);
                outV.setDate(date);

                outK.set(date);

                //5 写出
                context.write(outK,outV);
            }

        }
    }


    /**
     * Reduce阶段
     */
    public static class MyReducer extends Reducer<Text, hadoop.CarBean,Text, hadoop.CarBean>{
        Logger logger = LoggerFactory.getLogger(MyReducer.class);

        /**
         * 针对<k2,{v2...}>的数据进行累加求和，并且最终把数据转化为k3,v3写出去
         * @param k2
         * @param beans
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<hadoop.CarBean> beans, Context context)
                throws IOException, InterruptedException {
            //定义科目数量
            int count = 0;
            //创建一个sum变量，保存v2s的和
            double sumUp = 0d;
            double sumSpeed = 0d;
            //对v2s中的数据进行累加求和
            for(hadoop.CarBean bean: beans){
                //输出k2,v2的值
                sumUp += Double.valueOf(bean.getUpSpeed());
                sumSpeed += Double.valueOf(bean.getSpeed());
                count++;
            }

            //求平均数
            Double avg_up = sumUp / count;
            Double avg_speed = sumSpeed / count;




            //组装结果
            hadoop.CarBean rs =  new hadoop.CarBean();
            rs.setSpeed(avg_speed.toString());
            rs.setUpSpeed(avg_up.toString());

            System.err.println(rs.toString());
            // 把结果写出去
            context.write(k2,rs);
        }
    }

    /**
     * 组装Job=Map+Reduce
     */
    public static void main(String[] args) {
        try{

            //指定Job需要的配置参数
            Configuration conf = new Configuration();

            // 创建文件系统
            FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf);
            // 如果输出目录存在，我们就删除
            if (fileSystem.exists(new Path(OUT_PATH))) {
                fileSystem.delete(new Path(OUT_PATH), true);
            }



            //创建一个Job
            Job job = Job.getInstance(conf);

            //注意了：这一行必须设置，否则在集群中执行的时候是找不到WordCountJob这个类的
            job.setJarByClass(CarSttDayJob.class);
            
            //指定输入路径（可以是文件，也可以是目录）
            FileInputFormat.setInputPaths(job, INPUT_PATH);
            //指定输出路径(只能指定一个不存在的目录)
            FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

            //指定map相关的代码
            job.setMapperClass(MyMapper.class);
            //指定k2的类型
            job.setMapOutputKeyClass(Text.class);
            //指定v2的类型
            job.setMapOutputValueClass(hadoop.CarBean.class);


            //指定reduce相关的代码
            job.setReducerClass(MyReducer.class);
            //指定k3的类型
            job.setOutputKeyClass(Text.class);
            //指定v3的类型
            job.setOutputValueClass(hadoop.CarBean.class);

            //提交job
            job.waitForCompletion(true);
        }catch(Exception e){
            e.printStackTrace();
        }

    }


}
