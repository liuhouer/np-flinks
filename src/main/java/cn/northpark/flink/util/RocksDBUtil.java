package cn.northpark.flink.util;

import org.apache.commons.lang3.StringUtils;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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

    public static void main(String[] args) {
//        RocksDBUtil.getInstance().putStr("bruce","test");
        String bruce = RocksDBUtil.getInstance().getData("bruce").toString();
        System.out.println(bruce);
    }

}
