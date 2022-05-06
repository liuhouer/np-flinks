package cn.northpark.flink.opentsdb;


import cn.northpark.flink.NorthParkSTT;
import cn.northpark.flink.bean.StatisticsVO;
import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.util.UriEncoder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author bruce
 * NorthPark多维度分析统计请求日志
 * 每天把统计结果写入openTSDB
 */
@Slf4j
public class NorthParkSTT_openTSDB {

    public static void main(String[] args) throws Exception {

        //1.环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.read
        DataStreamSource<String> readTextFile = env.readTextFile("C:\\Users\\Bruce\\Desktop\\STT.log");


        InputStream is = NorthParkSTT.class.getClassLoader().getResourceAsStream("phoenix.properties");
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(is);

        env.getConfig().setGlobalJobParameters(parameterTool);


        //3.transform
        SingleOutputStreamOperator<StatisticsVO> map = readTextFile.map(new MapFunction<String, StatisticsVO>() {

            @Override
            public StatisticsVO map(String value) throws Exception {
                int start_index = value.indexOf("[Statistics Info]^");
                String replace_1 = value.substring(start_index).replace("[Statistics Info]^", "");



                StatisticsVO vo = JSON.parseObject(replace_1, StatisticsVO.class);

                //替换tsdb中的违规字符
                vo.url = vo.url.replace(":","-").replace("%","-").replace("(","-").replace(")","-");

                log.info("log--url----{}",vo.url);

                return vo;
            }

        }).filter(new FilterFunction<StatisticsVO>() {
            @Override
            public boolean filter(StatisticsVO value) throws Exception {
                return value.url != null;
            }
        });


        //统计url---

        map.flatMap(new FlatMapFunction<StatisticsVO, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(StatisticsVO value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value.url, 1));
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1).addSink(new RichSinkFunction<Tuple2<String, Integer>>() {

            private HiTSDB opentsdb = null;
            private HiTSDBConfig config;

            /**
             * 创建连接
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                config = HiTSDBConfig
                        .address("node1", 4242)
                        .readonly(false)
                        .asyncPut(true)
                        .listenBatchPut(new BatchPutCallback() {
                            @Override
                            public void response(String address, List<Point> input, Result output) {
                                log.info("success save into openTSDB, data size:" + input.size());
                            }

                            @Override
                            public void failed(String address, List<Point> input, Exception ex) {
                                log.error(ex.getMessage(), ex);
                                log.error("fail to save data into OpenTSDB:" + input.size());
                            }
                        })
                        .config();
                opentsdb = HiTSDBClientFactory.connect(config);
            }

            /**
             * 关闭连接
             * @throws Exception
             */
            @Override
            public void close() throws Exception {
                super.close();
                if (opentsdb != null) {
                    opentsdb.close(true);
                }
            }

            //写入opentsdb
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                List<Point> points = new ArrayList<>();
                Point highPoint = Point.metric("metric").tag("type", value.f0).value(System.currentTimeMillis(), value.f1).build();
                points.add(highPoint);

                DetailsResult detailsResult = opentsdb.putSync(points, DetailsResult.class);
                if (detailsResult.getFailed() > 0) {
                    log.error("Put records into openTSDB failed, count: {}", detailsResult.getFailed());
                    log.error(detailsResult.getErrors().toString());
                }
            }
        });

        //统计用户 user_5
//        map.filter(new FilterFunction<StatisticsVO>() {
//            @Override
//            public boolean filter(StatisticsVO vo) throws Exception {
//
//                return vo.userVO != null;
//            }
//        }).flatMap(new FlatMapFunction<StatisticsVO, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(StatisticsVO value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                out.collect(Tuple2.of(value.userVO.username, 1));
//            }
//        }).keyBy(0).sum(1).addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
//
//
//            private transient Connection conn;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//
//                Properties props = new Properties();
//                props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
//                props.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
//                String driverClassName = params.get("driverClassName");
//                String jdbcUrl = params.get("jdbcUrl");
//
//                Class.forName(driverClassName);
//                conn = DriverManager.getConnection(jdbcUrl,props);
//            }
//
//            @Override
//            public void close() throws Exception {
//                super.close();
//                if(conn !=null){
//                    conn.close();
//                }
//            }
//
//            @Override
//            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
//
//                String sql = "UPSERT INTO \"stt\".USER_STT_20211217 (USER_NAME,SCORE) VALUES ('"+value.f0+"',"+value.f1+")";
//                try {
//                    PreparedStatement ps = conn.prepareStatement(sql);
//                    String msg = ps.executeUpdate() >0 ? "插入成功..."
//                            :"插入失败...";
//                    conn.commit();
//                    log.info(msg);
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//
//        //统计用户+请求页面次数 userA_uri 6
//        map.filter(new FilterFunction<StatisticsVO>() {
//            @Override
//            public boolean filter(StatisticsVO vo) throws Exception {
//
//                return vo.userVO != null;
//            }
//        }).flatMap(new FlatMapFunction<StatisticsVO, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(StatisticsVO value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                out.collect(Tuple2.of(value.userVO.username + "_" + value.url, 1));
//            }
//        }).keyBy(0).sum(1).addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
//
//
//            private transient Connection conn;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//
//                Properties props = new Properties();
//                props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
//                props.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
//                String driverClassName = params.get("driverClassName");
//                String jdbcUrl = params.get("jdbcUrl");
//
//                Class.forName(driverClassName);
//                conn = DriverManager.getConnection(jdbcUrl,props);
//            }
//
//            @Override
//            public void close() throws Exception {
//                super.close();
//                if(conn !=null){
//                    conn.close();
//                }
//            }
//            @Override
//            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
//
//
//                String sql = "UPSERT INTO \"stt\".USER_ACTION_STT_20211217 (USER_URL,SCORE) VALUES ('"+value.f0+"',"+value.f1+")";
//                try {
//                    PreparedStatement ps = conn.prepareStatement(sql);
//                    String msg = ps.executeUpdate() >0 ? "插入成功..."
//                            :"插入失败...";
//                    conn.commit();
//                    log.info(msg);
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//
//        //google bot请求页面次数
//        map.filter(new FilterFunction<StatisticsVO>() {
//            @Override
//            public boolean filter(StatisticsVO vo) throws Exception {
//
//                return Objects.nonNull(vo.cookieMap)  && vo.cookieMap.toString().contains("Googlebot");
//            }
//        }).flatMap(new FlatMapFunction<StatisticsVO, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(StatisticsVO value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                out.collect(Tuple2.of("G_BOT" + "_" + value.url, 1));
//            }
//        }).keyBy(0).sum(1).addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
//
//            private transient Connection conn;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//
//                Properties props = new Properties();
//                props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
//                props.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
//                String driverClassName = params.get("driverClassName");
//                String jdbcUrl = params.get("jdbcUrl");
//
//                Class.forName(driverClassName);
//                conn = DriverManager.getConnection(jdbcUrl,props);
//            }
//
//            @Override
//            public void close() throws Exception {
//                super.close();
//                if(conn !=null){
//                    conn.close();
//                }
//            }
//
//            @Override
//            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
//
//                String sql = "UPSERT INTO \"stt\".GOOGLE_BOT_STT_20211217 (URL,SCORE) VALUES ('"+value.f0+"',"+value.f1+")";
//                try {
//                    PreparedStatement ps = conn.prepareStatement(sql);
//                    String msg = ps.executeUpdate() >0 ? "插入成功..."
//                            :"插入失败...";
//                    conn.commit();
//                    log.info(msg);
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        });

//		map.groupBy("url").sum(0).print();
        //4.execute
        env.execute("NorthParkSTT");

    }

}
