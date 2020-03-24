package cn.northpark.flink.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
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

    private HikariUtils() {

    }

    // 定义HikariDataSource类型的dataSource
    // 注意： 因为HikariDataSource类 实现了DataSource 接口。 因此 dataSource 即是HikariDataSource类型也是DataSource类型
    public static HikariDataSource dataSource;


    /**
     * 获取数据源
     *
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public static DataSource getDataSource() {

        try {
            // 因为dataSource是全局变量、默认初始化值为null 
            if (dataSource == null) {
                // 通过字节输入流 读取 配置文件  mysql.properties
                InputStream is = HikariUtils.class.getClassLoader().getResourceAsStream("mysql.properties");
                // 因为HikariConfig类不可以加载io，但是可以加载Properties。因此：将输入流is封装到props
                Properties props = new Properties();
                props.load(is);
                // 再将封装好的props 传入到HikariConfig 类中，得到 config对象
                HikariConfig config = new HikariConfig(props);
                // 将config对象传入给HikariDataSource ，返回dataSource

                dataSource = new HikariDataSource(config);
            }
            // 返回dataSource
            return dataSource;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 获取数据源
     * @param sourceType :mysql | oracle
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public static DataSource getDataSource(String sourceType) {

        try {
            // 因为dataSource是全局变量、默认初始化值为null
            if (dataSource == null) {
                // 通过字节输入流 读取 配置文件  mysql.properties
                InputStream is = HikariUtils.class.getClassLoader().getResourceAsStream(sourceType+".properties");
                // 因为HikariConfig类不可以加载io，但是可以加载Properties。因此：将输入流is封装到props
                Properties props = new Properties();
                props.load(is);
                // 再将封装好的props 传入到HikariConfig 类中，得到 config对象
                HikariConfig config = new HikariConfig(props);
                // 将config对象传入给HikariDataSource ，返回dataSource

                dataSource = new HikariDataSource(config);
            }
            // 返回dataSource
            return dataSource;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 通过数据源获取连接 | 默认读取mysql的数据源配置
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    /**
     * 通过数据源获取连接
     *  @param sourceType: mysql | oracle
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String sourceType) throws SQLException {
        return getDataSource(sourceType).getConnection();
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
            Connection connection = HikariUtils.getConnection("mysql");

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

