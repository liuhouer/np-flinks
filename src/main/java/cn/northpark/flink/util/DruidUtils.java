package cn.northpark.flink.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


/**
 * @author zhangyang
 * Druid数据库连接池工具类的设计
 */
@Slf4j
public class DruidUtils {
    /**
     * 默认配置文件名
     */
    private transient static String confile = "druid.properties";
    /**
     * 配置文件
     */
    private transient static Properties p = null;
    /**
     * 唯一dateSource，保证全局只有一个数据库连接池
     */
    private transient static DataSource dataSource = null;


    static {
        p = new Properties();
        InputStream inputStream = null;
        try {
            // java应用 读取配置文件
            inputStream = DruidUtils.class.getClassLoader().getResourceAsStream(confile);
            p.load(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                // ignore
            }
        } // end finally

        try {
            //通过工厂类获取DataSource对象
            dataSource = DruidDataSourceFactory.createDataSource(p);
        } catch (Exception e) {
            log.error("获取连接异常 ", e);
        }

    } // end static

    private DruidUtils() {
    }

    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();

    }


    /**
     * 关闭连接
     *
     * @param  con
     * @date : 2017-10-16 10:08:10
     */

}


