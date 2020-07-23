package cn.northpark.flink.util;

import cn.northpark.flink.WordCount;
import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author zhangyang
 * @date 2020年07月22日 17:51:06
 */
public class RocksDBUtil extends RocksFsBase{

    private static RocksDB rocksDB;

    static {
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true).setAllowMmapWrites(true);
        options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
        try {
            rocksDB = RocksDB.open(options, "/Users/bruce/Documents/workspace/np-flink/backEnd/rocks");
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    public RocksDBUtil() {
        super(rocksDB);
    }


    private volatile static RocksDBUtil instance = null;

    public  static RocksDBUtil getInstance() {
        if (instance == null) {
            synchronized (RocksDBUtil.class) {
                if (instance == null) {
                    instance = new RocksDBUtil();
                }
            }
        }
        return instance;
    }

    /**
     * entity demo to RocksDB by zhangyang
     */
    static class WordCount implements Serializable,RocksSaveable{
        public String word;
        public Integer counts;

        public WordCount() {
        }

        public WordCount(String word, Integer counts) {
            this.word = word;
            this.counts = counts;
        }


        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", counts=" + counts +
                    '}';
        }


       @Override
       public void save(RocksDB rocksDB) throws Exception {
           byte[] data = ObjectUtil.objectToBytes(this);
           rocksDB.put(ObjectUtil.objectToBytes(this.getClass().getSimpleName()),data);
       }

       @Override
       public void deleteFromRocks(RocksDB rocksDB) throws Exception {
           rocksDB.delete(ObjectUtil.objectToBytes(this.getClass().getSimpleName()));
       }
   }

    public static void main(String[] args) {
        RocksDBUtil.getInstance().putStr("bruce","test");
        String bruce = RocksDBUtil.getInstance().getData("bruce").toString();
        System.out.println(bruce);

        //entity demo to RocksDB
        WordCount wc = new WordCount("bruce",19);
        RocksDBUtil.getInstance().saveRocksNode(wc);

//        byte[] data = RocksDBUtil.getInstance().getRocksData(wc.getClass().getSimpleName().getBytes());
//
//        try {
//            Object o = ObjectUtil.bytesToObject(data);
//            System.out.println(o.toString());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        Object data = RocksDBUtil.getInstance().getData(wc.getClass().getSimpleName());

        System.out.println(data.toString());
        System.out.println(JSON.toJSONString(data));


    }

}
