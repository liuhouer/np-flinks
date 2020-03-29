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
    /** 默认配置文件名 */
    public static String confile = "druid.properties";
    /** 配置文件 */
    public static Properties p = null;
    /** 唯一dateSource，保证全局只有一个数据库连接池 */
    public static DataSource dataSource = null;


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



    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection() throws SQLException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new SQLException("获取连接时异常", e);
        }

    }


    /**
     * 关闭连接
     *
     * @param  con
     * @date : 2017-10-16 10:08:10
     */
    public static void close(Connection con)  {
        try {
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
           e.printStackTrace();
        }finally {
            try {
                if (con != null) {
                    con.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    } // end method


    //非自动提交时需要手动提交|回滚...====================================================

    /**
     * 提交事务
     */
    public static void commit(Connection conn)  {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                log.error("提交事物失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 事物回滚
     *
     * @param conn
     */
    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                log.error("事物回滚失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

}


