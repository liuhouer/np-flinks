package cn.northpark.flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
public class MysqlConnCsvUpdateAttractions {

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
                                (Double) row.getField(3), (Double) row.getField(4));
                    }
                });

        // 读取 Excel 数据并转换成 Tuple3<String, String, Tuple2<Double, Double>> 类型
        DataSet<Tuple3<String, String, Tuple2<Double, Double>>> excelData = env.readCsvFile("src/main/resources/A级景区经纬度.xlsx")
                .types(String.class, String.class, Double.class, Double.class)
                .map(new MapFunction<Tuple4<String, String, Double, Double>, Tuple3<String, String, Tuple2<Double, Double>>>() {
                    @Override
                    public Tuple3<String, String, Tuple2<Double, Double>> map(Tuple4<String, String, Double, Double> value) throws Exception {
                        return Tuple3.of(value.f0, value.f1, Tuple2.of(value.f2, value.f3));
                    }
                });

        // 关联 MySQL 数据和 Excel 数据
        DataSet<Row> result = attractions.leftOuterJoin(excelData)
                .where(0)
                .equalTo(0)
                .with(
                        new JoinFunction<Tuple5<String, String, String, Double, Double>, Tuple3<String, String, Tuple2<Double, Double>>, Row>() {
                            @Override
                            public Row join(Tuple5<String, String, String, Double, Double> first, Tuple3<String, String, Tuple2<Double, Double>> second) throws Exception {
                                Row row = new Row(5);
                                row.setField(0, first.f0);
                                row.setField(1, first.f1);
                                row.setField(2, first.f2);
                                row.setField(3, first.f3);
                                row.setField(4, first.f4);
                                if (second != null) {
                                    row.setField(3, second.f2.f0);
                                    row.setField(4, second.f2.f1);
                                }
                                return row;
                            }
                        });


        // 写入 MySQL 数据库
        JDBCOutputFormatBuilder outputFormatBuilder = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/trip")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("UPDATE t_attractions SET longitude = ?, latitude = ? WHERE name = ?")
                .setSqlTypes(new int[]{java.sql.Types.DOUBLE, java.sql.Types.DOUBLE, java.sql.Types.VARCHAR});

        result.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/trip")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("UPDATE t_attractions SET longitude = ?, latitude = ? WHERE name = ?")
                .setSqlTypes(new int[]{java.sql.Types.DOUBLE, java.sql.Types.DOUBLE, java.sql.Types.VARCHAR})
                .finish());

        env.execute("Update Attractions");
    }
}