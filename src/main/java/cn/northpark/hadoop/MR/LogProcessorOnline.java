package cn.northpark.hadoop.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author bruce
 * @date 2023年06月14日 20:56:46
 *
 * 编写MapReduce  java任务,我们将对日志数据进行如下处理。
 * 丢弃第2栏和第3栏，因为它们没有任何实际的数据。
 * 将第5栏，即HTTP请求信息那一栏拆分为三个字段，分别表示:方法、资源、协议。
 * 为方便后续导入Hive中，每个字段之间使用英文逗号分隔。
 * 输出结果存放于HDFS的另外一个目录
 *
 * 原始数据字段描述：远程主机IP   EMAIL 登录名  请求时间  HTTP请求信息  状态代码  发送字节数
 * 原始数据格式如下：
 * 131.235.141.48 - - [25/Sep/1995:14:13:22 -0600] "GET /images/logo.gif HTTP/1.0" 200 2273
 * educ012.usask.ca - - [25/Sep/1995:14:13:33 -0600] "GET /education/ HTTP/1.0" 200 7816
 * educ012.usask.ca - - [25/Sep/1995:14:13:40 -0600] "GET /education/edbldg.gif HTTP/1.0" 200 32812
 * lib74224.usask.ca - - [25/Sep/1995:14:13:44 -0600] "GET /archives/ HTTP/1.0" 200 2057
 * 131.235.141.48 - - [25/Sep/1995:14:13:49 -0600] "GET /search/people_uofs.html HTTP/1.0" 200 1111
 * ...
 *
 * 输出案例数据格式如下：
 * 1995-06-01 17:07:21,sabre47.sasknet.sk.ca,GET,/lycos/spider-icon.gif,HTTP/1.0,200,881
 * 1995-06-01 17:07:15,sabre47.sasknet.sk.ca,GET,/cgi-bin/pursuit-beta,HTTP/1.0,200,1240
 *
 *
 * //hadoop提交作业
 * 1.hadoop jar np_hadoop-2.0-SNAPSHOT-jar-with-dependencies.jar hadoop.LogProcessorOnline
 *
 * //列出输出文件列表
 * 2.hdfs dfs -ls /159/
 *
 * /查看输出结果
 * 3.hadoop fs -cat /159/output/output_0.txt
 *
 */
public class LogProcessorOnline {

    // 定义输入路径
    private static final String INPUT_PATH = "/159/xab.log";
    // 定义输出路径
    private static final String OUT_PATH = "/159/output/";

    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        private SimpleDateFormat inputDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
        private SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] fields = line.split(" ");
            if (fields.length >= 8) {
                String ip = fields[0];
                //解析 request
                String request = "";
                if(fields[7].contains("/")){
                    request = fields[5]+ " " +fields[6]+" " +fields[7];
                }else{
                    request = fields[5]+ " " +fields[6];
                }

                //解析时间线
                String timeLine = fields[3]+ " " +fields[4];
                timeLine = timeLine.replace("[","").replace("]","");

                try {
                    Date date = inputDateFormat.parse(timeLine);
                    timeLine =  outputDateFormat.format(date);
                } catch (ParseException e) {
                    e.printStackTrace();
                }


                String method   = "";
                String resource = "";
                String protocol = "";
                String[] requests = request.split(" ");
                if(requests.length>2){
                     method   = requests[0];
                     resource = requests[1];
                     protocol = requests[2];

                }else  if(requests.length==2){
                    method   = requests[0];
                    resource = requests[1];
                }
                String outputValue = "";
                if(fields.length==10) {
                    outputValue = String.join(",", ip, method, resource, protocol,
                            fields[8], fields[9]);
                }else if(fields.length==9){
                    outputValue = String.join(",", ip, method, resource, protocol,
                            fields[7],fields[8]);
                }
                outKey.set(timeLine);
                outValue.set(outputValue);
                context.write(outKey, outValue);

            }
        }
    }

    public static class LogReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    /**
     * 自定义输出文件的格式
     */
    public static class LogProcessorOutputFormat extends TextOutputFormat<Text, Text> {

        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
            // 获取任务的唯一ID
            TaskID taskId = context.getTaskAttemptID().getTaskID();
            // 获取任务的序号
            int taskIndex = taskId.getId();
            // 指定输出文件的名称
            String fileName = "output_" + taskIndex + ".txt";
            // 指定输出文件的路径
            Path outputDirectory = getOutputPath(context);
            return new Path(outputDirectory, fileName);
        }
    }


    public static void main(String[] args) throws Exception {
        //指定Job需要的配置参数
        Configuration conf = new Configuration();

        // 创建文件系统
        FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf);
        // 如果输出目录存在，我们就删除
        if (fileSystem.exists(new Path(OUT_PATH))) {
            fileSystem.delete(new Path(OUT_PATH), true);
        }

        Job job = Job.getInstance(conf, "Log Processor");

        // 设置输出分隔符为逗号
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

        job.setJarByClass(LogProcessorOnline.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        //map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //out
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(LogProcessorOutputFormat.class);

        //指定输入路径（可以是文件，也可以是目录）
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        //指定输出路径(只能指定一个不存在的目录)
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
