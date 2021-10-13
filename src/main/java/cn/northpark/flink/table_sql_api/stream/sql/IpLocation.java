package cn.northpark.flink.table_sql_api.stream.sql;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

public class IpLocation extends ScalarFunction {
    private List<Tuple4<Long, Long, String, String>> lines = Lists.newArrayList();

    @Override
    public void open(FunctionContext context) throws Exception {
        File cachedFile = context.getCachedFile("ip-rules");

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(cachedFile)));

        String line = null;

        while ((line = bufferedReader.readLine()) != null) {
            String[] fields = line.split("[|]");
            Long startNum = Long.parseLong(fields[2]);
            Long endNum = Long.parseLong(fields[3]);
            String province = fields[6];
            String city = fields[7];
            lines.add(Tuple4.of(startNum, endNum, province, city));
        }

    }

    //必须
    public Row eval(String ip) {
        Long ipNum = ip2Long(ip);
        return binarySearch(ipNum);
    }

    public static Long ip2Long(String dottedIP) {
        String[] addrArray = dottedIP.split("\\.");
        long num = 0;
        for (int i=0;i<addrArray.length;i++) {
            int power = 3-i;
            num += ((Integer.parseInt(addrArray[i]) % 256) * Math.pow(256,power));
        }
        return num;
    }


    private Row binarySearch(Long ipNum){

        Row result = null;
        int index = -1;
        int low = 0;
        int high = lines.size() -1;
        while (low<=high){
            int middle = (low + high) /2;
            if(ipNum>=lines.get(middle).f0 && ipNum<=lines.get(middle).f1){
                index = middle;
            }
            if(ipNum<lines.get(middle).f0){
                high = middle-1;
            }else{
                low = middle+1;
            }
        }
        if(index!=-1){
            Tuple4<Long, Long, String, String> tp4 = lines.get(index);
            result = Row.of(tp4.f2,tp4.f3);
        }
        return result;
    }

}
