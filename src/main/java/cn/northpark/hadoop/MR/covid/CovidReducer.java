package cn.northpark.hadoop.MR.covid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author bruce
 * @date 2023年03月20日 13:59:45
 */
public class CovidReducer extends Reducer<Text, Covid, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Covid> values, Context context) throws IOException, InterruptedException {
        int totalCases = 0;
        int totalDeaths = 0;

        for (Covid value : values) {
            totalCases += value.getNewCases();
            totalDeaths += value.getNewDeaths();
        }

        String result = "[" + totalCases + ", " + totalDeaths + "]";
        context.write(key, new Text(result));
    }
}