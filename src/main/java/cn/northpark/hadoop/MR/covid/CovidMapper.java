package cn.northpark.hadoop.MR.covid;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Objects;

/**
 * @author bruce
 * @date 2023年03月20日 13:58:54
 */
public class CovidMapper extends Mapper<LongWritable, Text, Text, Covid> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        String country = data[2];
        int newCases = Integer.parseInt(Objects.nonNull(data[4])?data[4]:"0");
        int newDeaths = Integer.parseInt(Objects.nonNull(data[6])?data[6]:"0");

        if (country.equals("China") || country.equals("United States of America")) {
            context.write(new Text(country), new Covid(newCases, newDeaths));
        }
    }
}
