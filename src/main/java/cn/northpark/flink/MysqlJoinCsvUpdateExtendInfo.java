package cn.northpark.flink;

import cn.northpark.flink.util.JDBCHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.HashSet;

/**
 * @author bruce
 * @date 2023年03月15日 17:39:12
 * mysql 关联 csv的数据集，更新mysql表的部分字段数据
 *
 */
public class MysqlJoinCsvUpdateExtendInfo {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //1.读取csv转为流


        //创建集合，作为数据源
        Collection<Tuple6<String, String, String, String, String, String>> messageCollection = new HashSet<>();
        BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/The_Travel.csv"));//换成你的文件名
        reader.readLine();//第一行信息，为标题信息，不用,如果需要，注释掉
        String line = null;
        //从csv文件中挑选部分数据创建GeoMessage对象
        while ((line = reader.readLine()) != null) {
            //地点,短评,天数,人均费用,人物,玩法
            String item[] = line.split(",");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
            String shortCmt = escapeEmoji(item[1]);
            shortCmt = shortCmt.replaceAll("[\\p{C}\\p{So}\uFE00-\uFE0F\\x{E0100}-\\x{E01EF}]+", "")
                    .replaceAll(" {2,}", " ");

            messageCollection.add(Tuple6.of(item[0],shortCmt,item[2],item[3],item[4],item[5]));
        }
        //给流环境设置数据源
        DataStream<Tuple6<String, String, String, String, String, String>> excelData = env.fromCollection(messageCollection);


        excelData.process(new ProcessFunction<Tuple6<String, String, String, String, String, String>, Object>() {

            private transient Connection connection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/trip?characterEncoding=UTF-8","root","123456");
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void processElement(Tuple6<String, String, String, String, String, String> value, Context ctx, Collector<Object> out) throws Exception {
                //地点,短评,天数,人均费用,人物,玩法
//                String upSQL = "UPDATE t_attractions SET short_comment = ?, suitable_days = ?, avg_cost = ?, suitable_persons = ?, suitable_play = ? WHERE name LIKE ? OR region LIKE ? OR province LIKE ?";
                String upSQL = "UPDATE t_attractions SET short_comment = ?, suitable_days = ?, avg_cost = ?, suitable_persons = ?, suitable_play = ? WHERE name LIKE '%"+value.f0+"%' and suitable_persons =''";

                try {
                    JDBCHelper.getInstance().executeUpdate(connection,upSQL,value.f1,value.f2,value.f3,value.f4,value.f5);
                }catch (Exception ignore){
                    System.err.println(value);
                }finally {
                    System.err.println("更新成功！！===="+value);
                }

            }
        });

        env.execute("Update Attractions Data");
    }

    /**
     * 过滤表情等特殊符号
     *
     * @param source
     * @return
     */
    public static String escapeEmoji(String source) {
        if (StringUtils.isNotBlank(source)) {
            StringBuilder buff = new StringBuilder(source.length());
            for (char codePoint : source.toCharArray()) {
                if (((codePoint == 0x0) || (codePoint == 0x9) || (codePoint == 0xA) || (codePoint == 0xD)
                        || ((codePoint >= 0x20) && (codePoint <= 0xD7FF))
                        || ((codePoint >= 0xE000) && (codePoint <= 0xFFFD)) || ((codePoint >= 0x10000) && (codePoint <= 0x10FFFF)))) {
                    buff.append(codePoint);
                }
            }
            return buff.toString();
        }
        return source;
    }

}