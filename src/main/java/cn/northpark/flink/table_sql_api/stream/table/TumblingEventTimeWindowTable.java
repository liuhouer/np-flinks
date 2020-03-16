package cn.northpark.flink.table_sql_api.stream.table;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 利用table api 结合事件时间划分滚动窗口
 */
public class TumblingEventTimeWindowTable {
    public static void main(String[] args) throws Exception {

        //实时dataStream api
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //实时Table执行上下文
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //时间,用户ID,商品ID,价格
        //1000,u1,p1,5
        //2000,u1,p1,5
        //2000,u3,p1,5
        //3000,u1,p1,3
        //9999,u2,p1,4
        //19999,u1,p1,5
        DataStreamSource<String> lines = env.socketTextStream("localhost", 4000);

        SingleOutputStreamOperator<Row> rowSingleOutputStreamOperator = lines.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] fields = value.split(",");
                Long time = Long.parseLong(fields[0]);
                String uid = fields[1];
                String pid = fields[2];
                Double money = Double.parseDouble(fields[3]);
                return Row.of(time, uid, pid, money);
            }
        }).returns(Types.ROW(Types.LONG, Types.STRING, Types.STRING, Types.DOUBLE));

        SingleOutputStreamOperator<Row> watermarksRow = rowSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Row row) {
                        return (long) row.getField(0);
                    }
                });

        //将dataStream注册成表
        //rowtime.rowtime是事件时间  ：固定写法
        tableEnv.registerDataStream("t_orders", watermarksRow, "atime,uid,pid,money,rowtime.rowtime");

        Table table = tableEnv.scan("t_orders")
                .window(Tumble.over("10.seconds").on("rowtime").as("window"))
                .groupBy("uid,window")
                .select("uid,window.start,window.end,window.rowtime,money.sum as total");

        tableEnv.toAppendStream(table, Row.class).print();
        env.execute("TumblingEventTimeWindowTable");
    }
}
