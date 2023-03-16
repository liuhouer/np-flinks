package cn.northpark.flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author bruce
 * @date 2023年03月15日 17:39:12
 * mysql 关联 csv的数据集，更新mysql表的部分字段数据
 *
 */
public class MysqlJoinCsvUpdateFields {

    public static void main(String[] args) throws Exception {

        // 设置 MySQL 数据库连接属性
        Properties properties = new Properties();
        properties.setProperty("driverClassName", "com.mysql.jdbc.Driver");
        properties.setProperty("jdbcUrl", "jdbc:mysql://localhost:3306/trip");
        properties.setProperty("username", "root");
        properties.setProperty("password", "123456");

        // 创建 MySQL 输入格式
        JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/trip")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("SELECT name, region, category, longitude, latitude FROM t_attractions")
                .setRowTypeInfo(new RowTypeInfo(org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.DOUBLE,
                        org.apache.flink.api.common.typeinfo.Types.DOUBLE))
                .finish();

        // 创建 Flink 批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取 MySQL 数据并转换成 Tuple5<String, String, String, Double, Double> 类型
        DataSet<Tuple5<String, String, String, Double, Double>> attractions = env.createInput(inputFormat)
                .map(new MapFunction<Row, Tuple5<String, String, String, Double, Double>>() {
                    @Override
                    public Tuple5<String, String, String, Double, Double> map(Row row) throws Exception {
                        return Tuple5.of((String) row.getField(0), (String) row.getField(1), (String) row.getField(2),
                                0d, 0d);
                    }
                });

        System.err.println("mysql条数：：：：：：："+attractions.count());


        // 读取 Excel 数据并转换成 Tuple3<String, String, Tuple2<Double, Double>> 类型
        DataSet<Tuple4<String, String,String, Tuple2<Double, Double>>> excelData = env.readCsvFile("src/main/resources/A级景区经纬度.csv")
                .fieldDelimiter(",")
                .ignoreInvalidLines()
                .ignoreFirstLine()
                .types(String.class, String.class,String.class, Double.class, Double.class)
                .map(new MapFunction<Tuple5<String, String,String, Double, Double>, Tuple4<String, String,String, Tuple2<Double, Double>>>() {
                    @Override
                    public Tuple4<String, String, String, Tuple2<Double, Double>> map(Tuple5<String, String, String, Double, Double> value) throws Exception {
                        return Tuple4.of(value.f0, value.f1, value.f2, Tuple2.of(value.f3, value.f4));
                    }
                });


        // 关联 MySQL 数据和 Excel 数据
        DataSet<Row> result = attractions.leftOuterJoin(excelData)
                .where(0)
                .equalTo(0)
                .with(
                        new JoinFunction<Tuple5<String, String, String, Double, Double>, Tuple4<String, String,String, Tuple2<Double, Double>>, Row>() {
                            @Override
                            public Row join(Tuple5<String, String, String, Double, Double> first, Tuple4<String, String, String, Tuple2<Double, Double>> second) throws Exception {
                                Row row = new Row(3);  // 有3个参数
                                if(second!=null){
                                    row.setField(0, second.f3.f0);
                                    row.setField(1, second.f3.f1);
                                    row.setField(2, first.f0);
                                    System.out.println("============================>"+first.f0);
                                }else{
                                    row.setField(0, null);
                                    row.setField(1, null);
                                    row.setField(2, first.f0);
                                    System.err.println("！！！！！！！！！！！！！！！！！=》"+first.f0);
                                }

                                return row;
                            }
                        });


//        result.print();


        // 写入 MySQL 数据库
        JDBCOutputFormatBuilder outputFormatBuilder = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/trip")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("UPDATE t_attractions SET longitude = ?, latitude = ? WHERE name = ?")
                .setSqlTypes(new int[]{java.sql.Types.DOUBLE, java.sql.Types.DOUBLE, java.sql.Types.VARCHAR});

        result.output(outputFormatBuilder.finish());

        env.execute();

        System.out.println("MySQL写入成功！");
    }

}