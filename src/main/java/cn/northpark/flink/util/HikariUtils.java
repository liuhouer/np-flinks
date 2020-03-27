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
    /** 配置文件 */
    public static Properties p = null;
    /** 唯一dateSource，保证全局只有一个数据库连接池 */
    public static HikariDataSource dataSource = null;

    static{
        p = new Properties();
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
        }catch (Exception e) {
            log.error("获取连接异常 ", e);
        }
    }


    /**
     * 通过数据源获取连接
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new SQLException("获取连接时异常", e);
        }
    }

    /**
     * jdbc原生获取连接
     *
     * @param url
     * @param user
     * @param password
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String url, String user, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("获取mysql.jdbc.Driver失败");
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(url, user, password);
            log.info("获取连接:{} 成功...",conn);
        }catch (Exception e){
            log.error("获取连接失败，url:" + url + ",user:" + user);
        }

        //设置手动提交
        conn.setAutoCommit(false);
        return conn;
    }


    //非自动提交时需要手动提交|回滚...====================================================

    /**
     * 提交事务
     */
    public static void commit(Connection conn) {
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


    /**
     * 关闭连接
     *
     * @param conn
     */
    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("关闭连接失败,Connection:" + conn);
                e.printStackTrace();
            }
        }
    }





    //非自动提交时需要手动提交|回滚...====================================================


    public static void main(String[] args) {

        try {
            Connection connection = HikariUtils.getConnection();

            String sql = "insert into t_word_counts values (?,?,?)";
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, UUID.randomUUID().toString());
            ps.setString(2, "JAVA");
            ps.setString(3, "3");
            ps.executeUpdate();

            HikariUtils.commit(connection);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}

