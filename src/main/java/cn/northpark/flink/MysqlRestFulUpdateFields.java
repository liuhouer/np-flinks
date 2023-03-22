package cn.northpark.flink;

import cn.hutool.core.codec.Base64;
import cn.hutool.http.HttpUtil;
import cn.northpark.flink.util.JDBCHelper;
import cn.northpark.flink.util.JsonUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

/**
 * @author bruce
 * @date 2023年03月15日 17:39:12
 * mysql 关联 csv的数据集，更新mysql表的部分字段数据
 *
 */
public class MysqlRestFulUpdateFields {

    static final String KEY  = Base64.decodeStr("NzRhODVkNjFmYTkyYWZlMzViYzY2YmUyZjk1ZjBjMTY=");
    static final String KEY2 = Base64.decodeStr("MmE2MTA1MGRhMTc5YTA0NWMxNzRkMDZjZDFhNGI0MDM=");
    static final String KEY3 = Base64.decodeStr("OWM2OTAyYTdmNzVhZjY0MjhiOGNlYjlkMmMzYmQxNzU=");

    public static void main(String[] args) throws Exception {


        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =  EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv,settings);

        // 构建连接信息
        String url = "jdbc:mysql://localhost:3306/trip?characterEncoding=utf-8&useSSL=false";
        String user = "root";
        String password = "123456";

        //SELECT name, longitude, latitude FROM t_attractions

        // 注册 MySQL 表，并使用连接信息作为源表
        tEnv.executeSql(String.format("CREATE TABLE t_attractions (\n" +
                "    name VARCHAR(255),\n" +
                "    longitude DECIMAL(10, 6),\n" +
                "    latitude DECIMAL(10, 6)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = '%s',\n" +
                "    'table-name' = 't_attractions',\n" +
                "    'username' = '%s',\n" +
                "    'password' = '%s'\n" +
                ")", url, user, password));

        // 执行查询，并转换为 Tuple3
        Table resultTable = tEnv.sqlQuery("SELECT name, CAST(longitude AS String)as longitude , CAST(latitude as String)as latitude FROM t_attractions where longitude is null");
//        Table resultTable = tEnv.sqlQuery("SELECT name,  longitude ,   latitude FROM t_attractions ");

//        resultTable.printSchema();

        // 将 Table 转换为流
        DataStream<Tuple3<String, String , String>> mysqlStream = tEnv.toRetractStream(resultTable, TypeInformation.of(new TypeHint<Tuple3<String, String, String>>(){}))
                .filter(t -> t.f0) // 过滤掉被撤回的数据
                .map(t -> t.f1)
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
                ; // 获取元组的第二个元素，也就是 Row

        // 打印结果
        mysqlStream.print();

        mysqlStream.process(new ProcessFunction<Tuple3<String, String, String>, Object>() {
            private transient Connection connection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/trip?characterEncoding=UTF-8","root","123456");

            }

            @Override
            public void close() throws Exception {
                super.close();
                connection.close();
            }

            @Override
            public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Object> out) throws Exception {

                //处理不是空 并且是0的
                if(StringUtils.isBlank(value.f1) ){
                    String name = value.f0;
                    name = name.replace("【","").replace("】","").replace("?","").replace("-","").replace("《","").replace("》","").replace("——","");
                    String baseUrl = "https://restapi.amap.com/v3/geocode/geo?address="+ name +"&key="+KEY3;
                          String res = null;
                          while (StringUtils.isBlank(res)){
                              res = HttpUtil.get(baseUrl);
                          }
                          try {
                              Map<String, Object> json2map = JsonUtil.json2map(res);
                              System.err.println(json2map);
                              if(json2map.get("info").toString().equals("OK")){
                                  Object geocodes = json2map.get("geocodes");
                                  JSONArray jsonArray = JSON.parseArray(geocodes.toString());
                                  Map<String, Object> geocodesMap = JsonUtil.json2map(jsonArray.get(0).toString());
                                  String location = geocodesMap.get("location").toString();
                                  String longitude = location.split(",")[0];
                                  String latitude = location.split(",")[1];
                                  System.err.println(longitude);
                                  System.err.println(latitude);

                                  JDBCHelper.getInstance().executeUpdate(connection,"UPDATE t_attractions SET longitude = ?, latitude = ? WHERE name = ?",longitude,latitude, value.f0);

                              }
                          }catch (Exception e){
                              System.err.println("parseError====>"+res);
                          }

                }

            }
        });



//        tEnv.toRetractStream(resultTable, Row.class).print();
//        SingleOutputStreamOperator<Tuple3> mapStream = tEnv.toRetractStream(resultTable, Tuple3.class).map(new MapFunction<Tuple2<Boolean, Tuple3>, Tuple3>() {
//            @Override
//            public Tuple3 map(Tuple2<Boolean, Tuple3> value) throws Exception {
//                return value.f1;
//            }
//        });

//        mapStream.print();


//        System.err.println("mysql条数：：：：：：："+attractions.count());



//        https://restapi.amap.com/v3/geocode/geo?address=圆明园&key=74a85d61fa92afe35bc66be2f95f0c16


        // 写入 MySQL 数据库
//        JDBCOutputFormatBuilder outputFormatBuilder = JDBCOutputFormat.buildJDBCOutputFormat()
//                .setDrivername("com.mysql.jdbc.Driver")
//                .setDBUrl("jdbc:mysql://localhost:3306/trip")
//                .setUsername("root")
//                .setPassword("123456")
//                .setQuery("UPDATE t_attractions SET longitude = ?, latitude = ? WHERE name = ?")
//                .setSqlTypes(new int[]{java.sql.Types.DOUBLE, java.sql.Types.DOUBLE, java.sql.Types.VARCHAR});

//        result.output(outputFormatBuilder.finish());

        streamEnv.execute("MysqlRestFulUpdateFields");

    }

}