package cn.northpark.flink.util;

import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * @author zhangyang
 * @date 2020年07月22日 17:51:06
 */
public class RocksFsBase {


    protected RocksDB rocksDB;

    public RocksFsBase(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }


    protected boolean isByteArrayEqual(byte[] keyBegin, byte[] keyEnd) {
        if (keyBegin == keyEnd) {
            return true;
        }

        if (keyBegin != null && keyEnd != null && keyBegin.length == keyEnd.length) {
            int length = keyBegin.length;
            for (int i = 0; i < length; i++) {
                byte b_1 = keyBegin[i];
                byte b_2 = keyEnd[i];
                if (b_1 != b_2) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    protected boolean isEmpty(byte[] byteArray) {
        return byteArray == null || byteArray.length == 0;
    }

    protected Object getData(String key) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        try {
            byte[] bytes = rocksDB.get(ObjectUtil.objectToBytes(key));
            return ObjectUtil.bytesToObject(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    protected void putStr(String key, Object data) {
        try {
            rocksDB.put(ObjectUtil.objectToBytes(key), ObjectUtil.objectToBytes(data));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected byte[] getRocksData(byte[] key) {
        if (isEmpty(key)) {
            return null;
        }
        try {
            return rocksDB.get(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected void putRocksData(byte[] key, byte[] data) {
        try {
            rocksDB.put(key, data);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void saveRocksNode(RocksSaveable rocksSaveable) {
        if (rocksSaveable != null) {
            try {
                rocksSaveable.save(rocksDB);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public void deleteRocksNode(RocksSaveable rocksSaveable) {
        if (rocksSaveable != null) {
            try {
                rocksSaveable.deleteFromRocks(rocksDB);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
