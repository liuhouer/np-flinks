package cn.northpark.flink.table_sql_api;

import cn.northpark.flink.bean.Covid;
import cn.northpark.flink.util.KafkaString;
import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author bruce
 * @date 2022年05月08日 22:52:34
 */
public class ConvertCSV2KafkaBean {
    public static void main(String[] args) {
        String csvFile = "C:\\Users\\Bruce\\Desktop\\today_province_2022_04_15.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new FileReader(csvFile));
            br.readLine(); // 提前读一下就跳过了
            while ((line = br.readLine()) != null) {
                String[] split = line.split(",");
                String  var0  = split[0];
                String  var1  = split[1];
                String  var2  = split[2];
                String var3  = split[3] ;
                String var4  = split[4] ;
                String var5  = split[5] ;
                String var6  = split[6] ;
                String var7  = split[7] ;
                String var8  = split[8] ;
                String var9  = split[9] ;
                String var10 = split[10];
                String var11 = split[11];
                String var12 = split[12];
                String var13 = split[13];
                String var14 = split[14];
                String var15 = split[15];
                String var16 = split[16];
                String var17 = split[17];

                Covid bean = new Covid(var0,var1,var2,var3,var4,var5,
                        var6,var7,var8,var9,var10
                        ,var11,var12,var13,var14,var15,var16,var17);

                System.err.println(bean.toString());



                KafkaString.sendKafkaString(KafkaString.buildBasicKafkaProperty(),"covid19", JSON.toJSONString(bean));
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
