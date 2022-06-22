package cn.northpark.flink.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author bruce
 * @date 2022年06月22日 13:35:42
 */
public class MongoUtils {
    public static MongoClient getConnect(){
        List<ServerAddress> adds = new ArrayList<ServerAddress>();

        ServerAddress serverAddress = new ServerAddress("node1", 27017);
        adds.add(serverAddress);

        List<MongoCredential> credentials = new ArrayList<>();

        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential("root", "admin", "123456".toCharArray());
        credentials.add(mongoCredential);

        // 通过连接认证获取MongoDB连接
        MongoClient mongoClient2 = new MongoClient(adds, credentials);

        return mongoClient2;
    }


    //======================================================================================================================
    /**
     * 插入一个文档
     * @author zql
     * @createTime 2020-11-30 23:52:36
     *
     * @param data 要插入的数据
     * @param mongoDatabase 连接数据库对象
     * @param col 插入的集合，如果指定的集合不存在，mongoDB将会在第一次插入文档时创建集合。
     */
    public static void insertOne (Map<String, Object> data, MongoDatabase mongoDatabase, String col) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);

        //创建文档
        Document document = new Document();

        for (Map.Entry<String, Object> m : data.entrySet()) {
            document.append(m.getKey(), m.getValue()).append(m.getKey(), m.getValue());
        }

        //插入一个文档
        collection.insertOne(document);
    }

    /**
     * 插入多个文档
     * @author zql
     * @createTime 2020-11-30 23:52:17
     *
     * @param listData 要插入的数据
     * @param mongoDatabase 连接数据库对象
     * @param col 插入的集合，如果指定的集合不存在，mongoDB将会在第一次插入文档时创建集合。
     */
    public static void insertMany (List<Map<String, Object>> listData, MongoDatabase mongoDatabase, String col) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);

        //要插入的数据
        List<Document> list = new ArrayList<>();
        for (Map<String, Object> data : listData) {
            //创建文档
            Document document = new Document();

            for (Map.Entry<String, Object> m : data.entrySet()) {
                document.append(m.getKey(), m.getValue());
            }
            list.add(document);
        }

        //插入多个文档
        collection.insertMany(list);
    }

    /**
     * 删除匹配到的第一个文档
     * @author zql
     * @createTime 2020-11-30 23:51:56
     *
     * @param col 删除的集合
     * @param key 删除条件的键
     * @param value 删除条件的值
     * @param mongoDatabase 连接数据库对象
     */
    public static void delectOne (String col, String key, Object value, MongoDatabase mongoDatabase) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);
        //申明删除条件
        Bson filter = Filters.eq(key, value);
        //删除与筛选器匹配的单个文档
        collection.deleteOne(filter);
    }

    /**
     * 删除匹配的所有文档
     * @author zql
     * @createTime 2020-11-30 23:50:59
     *
     * @param col 删除的集合
     * @param key 删除条件的键
     * @param value 删除条件的值
     * @param mongoDatabase 连接数据库对象
     */
    public static void deleteMany (String col, String key, Object value, MongoDatabase mongoDatabase) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);
        //申明删除条件
        Bson filter = Filters.eq(key, value);
        //删除与筛选器匹配的所有文档
        collection.deleteMany(filter);
    }

    /**
     * 删除集合中所有文档
     * @author zql
     * @createTime 2020-11-30 23:51:19
     *
     * @param col
     * @param mongoDatabase
     */
    public static void deleteAllDocument(String col, MongoDatabase mongoDatabase) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);
        collection.deleteMany(new Document());
    }

    /**
     * 删除文档和集合。
     * @author zql
     * @createTime 2020-11-30 23:51:27
     *
     * @param col
     * @param mongoDatabase
     */
    public static void deleteAllCollection(String col, MongoDatabase mongoDatabase) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);
        collection.drop();
    }

    /**
     * 修改单个文档，修改过滤器筛选出的第一个文档
     * @author zql
     * @createTime 2020-11-30 23:50:12
     *
     * @param col 修改的集合
     * @param key 修改条件的键
     * @param value 修改条件的值
     * @param eqKey 要修改的键，如果eqKey不存在，则新增记录
     * @param eqValue 要修改的值
     * @param mongoDatabase 连接数据库对象
     */
    public static void updateOne (String col, String key, Object value,String eqKey, Object eqValue, MongoDatabase mongoDatabase) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);
        //修改过滤器
        Bson filter = Filters.eq(key, value);
        //指定修改的更新文档
        Document document = new Document("$set", new Document(eqKey, eqValue));
        //修改单个文档
        collection.updateOne(filter, document);

        //    MongoConnection.updateOne("user", "姓名", "张三","姓名改", "张三改1", mongoDatabase);
    }

    /**
     * 修改多个文档
     * @author zql
     * @createTime 2020-11-30 23:49:40
     *
     * @param col 修改的集合
     * @param key 修改条件的键
     * @param value 修改条件的值
     * @param eqKey 要修改的键，如果eqKey不存在，则新增记录
     * @param eqValue 要修改的值
     * @param mongoDatabase 连接数据库对象
     */
    public static void updateMany (String col, String key, Object value, String eqKey, Object eqValue, MongoDatabase mongoDatabase) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);
        //修改过滤器
        Bson filter = Filters.eq(key, value);
        //指定修改的更新文档
        Document document = new Document("$set", new Document(eqKey, eqValue));
        //修改多个文档
        collection.updateMany(filter, document);
    }

    /**
     * 查找集合中的所有文档
     * @author zql
     * @createTime 2020-11-30 23:49:15
     *
     * @param col 要查询的集合
     * @param mongoDatabase 连接数据库对象
     * @return
     */
    public static MongoCursor<Document> find (String col, MongoDatabase mongoDatabase) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);
        //查找集合中的所有文档
        FindIterable<Document> findIterable = collection.find();
        MongoCursor<Document> cursorIterator = findIterable.iterator();
        return cursorIterator;
    }

    /**
     * 按条件查找集合中文档
     * @author zql
     * @createTime 2020-11-30 23:48:44
     *
     * @param col 要查询的集合
     * @param key
     * @param value
     * @param mongoDatabase 连接数据库对象
     * @return
     */
    public static MongoCursor<Document> Filterfind (String col,String key, Object value, MongoDatabase mongoDatabase) {
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(col);
        //指定查询过滤器
        Bson filter = Filters.eq(key, value);

        //指定查询过滤器查询
        FindIterable<Document> findIterable = collection.find(filter);
        MongoCursor<Document> cursorIterator = findIterable.iterator();
        return cursorIterator;
    }


    public static void main(String[] args) {
        MongoClient connect = MongoUtils.getConnect();

        System.err.println(connect);

        // 获取数据库连接对象
//        Map<String, Object> m1 = new HashMap<String, Object>();
//        m1.put("姓名", "张三");
//        m1.put("性别", "男");
//        m1.put("年龄", 18);
//        Map<String, Object> m2 = new HashMap<String, Object>();
//        m2.put("姓名", "李四");
//        m2.put("性别", "女");
//        m2.put("年龄", 17);
//        List<Map<String, Object>> listData = new ArrayList<>();
//        listData.add(m1);
//        listData.add(m2);
//        MongoUtils.insertMany(listData, connect.getDatabase("flink"), "t_t_user");

        //更新一个条目
//        MongoUtils.updateOne("t_t_user", "姓名", "张三","性别", "女", connect.getDatabase("flink"));


        //获取集合
        MongoCollection<Document> collection = connect.getDatabase("flink").getCollection("t_t_user");
        //修改过滤器
        Bson filter = Filters.eq("姓名", "张三");

        Document doc = new Document();
        doc.append("年龄", 19);
        //修改单个文档
        collection.replaceOne(filter, doc, new UpdateOptions().upsert(true));


        if(connect!=null){
            connect.close();
        }


    }
}
