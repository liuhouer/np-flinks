package cn.northpark.flink.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;


/**
 * @author zhangyang
 * Hikaricp数据库连接池工具类的设计
 */
@Slf4j
public class HikariUtils {

    // 定义HikariDataSource类型的dataSource
    // 注意： 因为HikariDataSource类 实现了DataSource 接口。 因此 dataSource 即是HikariDataSource类型也是DataSource类型
    /**
     * 配置文件
     */
    private static transient Properties p = new Properties();
    /**
     * 唯一dateSource，保证全局只有一个数据库连接池
     */
    private static transient HikariDataSource dataSource = null;

    static {
        InputStream inputStream = null;
        try {
            // java应用 读取配置文件
            inputStream = HikariUtils.class.getClassLoader().getResourceAsStream("hikari.properties");
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
            HikariConfig config = new HikariConfig(p);
            dataSource = new HikariDataSource(config);
        } catch (Exception e) {
            log.error("获取连接异常 ", e);
        }
    }


    private HikariUtils() {
    }

    /**
     * 通过数据源获取连接
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

//非自动提交时需要手动提交|回滚...====================================================


    public static void main(String[] args) {


    }

}

