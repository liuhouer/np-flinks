package cn.northpark.spark.scoreApp;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author bruce
 * @date 2022年06月15日 15:19:41
 * 编写Java程序，利用IO流向d:\\hadoop\score.txt写入5个同位3科成绩
 * 追加模式写入
 */
public class sinkScoreAppended {
    private static final String sinkDir = "c:///Users/Bruce/Desktop/5/score.txt";


    public static void main(String[] args) {
        List<String> list = Arrays.asList(
                String.join(",", "4", "马云", "3403", "家园的治理：环境科学概论", "92", "2022年6月15日"),
                String.join(",", "4", "马云", "B0021001", "军事理论", "88", "2022年6月15日"),
                String.join(",", "4", "马云", "3509", "创业创新领导力", "76", "2022年6月14日"),
                String.join(",", "5", "赵散散", "3403", "家园的治理：环境科学概论", "89", "2022年6月15日"),
                String.join(",", "5", "赵散散", "B0021001", "军事理论", "82", "2022年6月15日"),
                String.join(",", "5", "赵散散", "3509", "创业创新领导力", "93", "2022年6月14日"),
                String.join(",", "6", "李科及", "3403", "家园的治理：环境科学概论", "66", "2022年6月15日"),
                String.join(",", "6", "李科及", "B0021001", "军事理论", "97", "2022年6月15日"),
                String.join(",", "6", "李科及", "3509", "创业创新领导力", "92", "2022年6月14日")

        );
        FileWriter writer = null;
        try {

            File file  = new File(sinkDir);
            if(!file.getParentFile().exists()){
                boolean result = file.getParentFile().mkdirs();
                if(!result){
                    throw new RuntimeException("创建文件路径失败");
                }
            }
            //第二个参数为true则追加
            writer = new FileWriter(file,true);

            for (String str : list) {
                writer.write(str);
                writer.write("\n");
            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(Objects.nonNull(writer)){
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }

    }
}
