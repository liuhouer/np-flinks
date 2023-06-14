package cn.northpark.hadoop.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
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
 */
public class LogProcessor {
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


    public static void main(String[] args) throws Exception {
        //app配置
        Configuration conf = new Configuration();
        //本地执行
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "Log Processor");

        // 设置输出分隔符为逗号
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

        job.setJarByClass(LogProcessor.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        //map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //out
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("C:\\Users\\Bruce\\Desktop\\xaa.log"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Bruce\\Desktop\\output2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
