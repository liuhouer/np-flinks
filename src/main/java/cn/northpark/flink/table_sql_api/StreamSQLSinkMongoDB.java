package cn.northpark.flink.table_sql_api;

import cn.northpark.flink.WordCount;
import cn.northpark.flink.util.MongoUtils;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author bruce
 * @date 2022年06月22日 13:27:10
 */
public class StreamSQLSinkMongoDB {

    public static void main(String[] args) throws Exception {

        //实时dataStream api
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //实时Table执行上下文
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //word count java scala
        DataStreamSource<String> lines = env.socketTextStream("node1", 8888);

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                Arrays.stream(value.split(" ")).forEach(out::collect);
            }
        });

        //注册程表
        tableEnv.registerDataStream("t_word_count",words,"word");

        //写sql
        Table table = tableEnv.sqlQuery("select word,cast(count(1) as int) counts from t_word_count group by word");

        //sql 转流
        DataStream<Tuple2<Boolean, WordCountBean>> tuple2DataStream = tableEnv.toRetractStream(table, WordCountBean.class);

        tuple2DataStream.print();

        //写入mongo db
        tuple2DataStream.addSink(new RichSinkFunction<Tuple2<Boolean, WordCountBean>>() {
            private transient MongoClient mongoClient = null;

            public void open(Configuration parms) throws Exception {
                super.open(parms);
                mongoClient = MongoUtils.getConnect();
            }

            public void close() throws Exception {
                if (mongoClient != null) {
                    mongoClient.close();
                }
            }

            @Override
            public void invoke(Tuple2<Boolean, WordCountBean> value, Context context) throws Exception {
                try {
                    if (mongoClient != null) {
                        Boolean appended = value.f0;
                        WordCountBean wordCount = value.f1;
                        mongoClient = MongoUtils.getConnect();
                        MongoDatabase db = mongoClient.getDatabase("flink");
                        MongoCollection collection = db.getCollection("t_word_count");
//                        List<Document> list = new ArrayList<>();

                        //新增
                        if(appended){
                            //修改过滤器--[word=flink]这里设置为主键或者key by 的键+值
                            Bson filter = Filters.eq("word", wordCount.word);

                            Document doc = new Document();
                            doc.append("word", wordCount.word);
                            doc.append("counts", wordCount.counts);
                            System.out.println("Insert Starting");

                            collection.replaceOne(filter, doc, new UpdateOptions().upsert(true));

                        }
                        //false的数据跳过不处理

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
            }
        });


        env.execute("StreamSQLSinkMongoDB");

    }


}
