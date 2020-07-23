package cn.northpark.flink.util;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;

/**
 * @author zhangyang
 * @date 2020年07月22日 17:58:03
 */
public interface RocksSaveable {
    void save(RocksDB rocksDB) throws Exception;

    void deleteFromRocks(RocksDB rocksDB) throws Exception;
}
