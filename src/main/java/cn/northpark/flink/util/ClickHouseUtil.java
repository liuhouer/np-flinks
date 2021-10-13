package cn.northpark.flink.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 * clickhouse jdbc操作工具类
 * @author bruce
 * @date 2021年10月13日
 */
@Slf4j
public class ClickHouseUtil {

    private ClickHouseUtil() {}

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
            inputStream = ClickHouseUtil.class.getClassLoader().getResourceAsStream("clickhouse.properties");
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


    /**
     * 通过数据源获取连接
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection()  {
        try {
            return dataSource.getConnection();
        }catch (Exception e){
            return null;
        }

    }

    public static void closeConnection(Connection conn){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表\插入\删除 等执行sql的操作
     */
    public static void execSql(String sql) {
        Connection conn = getConnection();
        try {
            PreparedStatement sta = conn.prepareStatement(sql);
            sta.execute();
            System.out.println("执行成功...");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                closeConnection(conn);
            }
        }

    }


    /**
     * 获取ck中的表
     */
    public static List<String> getTables(String scama) {
        Connection conn = getConnection();
        List<String> tables = new ArrayList<>();
        try{
            DatabaseMetaData metaData = conn.getMetaData();
            String[] types = {"TABLE"}; //"SYSTEM TABLE"
            ResultSet resultSet = metaData.getTables(null, scama, null, types);
            while (resultSet.next()) {
                tables.add(resultSet.getString("TABLE_NAME"));
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null) {
                closeConnection(conn);
            }
        }
        return tables;
    }
    /**
     * 获取表中的所有数据
     */
    public static List<Map<String, String>> getList(String tableName)  {
        String sql = "SELECT * FROM " + tableName;
        Connection conn = getConnection();
        List<Map<String, String>> resultList = new ArrayList<>();
        try{
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            while (resultSet.next()) {
                Map<String, String> result = new HashMap<>();
                for (int i = 1, len = resultSetMetaData.getColumnCount(); i <= len; i++) {
                    result.put(resultSetMetaData.getColumnName(i), resultSet.getString(i));
                }
                resultList.add(result);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null) {
                closeConnection(conn);
            }
        }
        return resultList;
    }

    /**
     * 查询sql返回map集合
     */
    public static List<Map<String, String>> query(String sql)  {
        Connection conn = getConnection();
        List<Map<String, String>> resultList = new ArrayList<>();
        try{
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            while (resultSet.next()) {
                Map<String, String> result = new HashMap<>();
                for (int i = 1, len = resultSetMetaData.getColumnCount(); i <= len; i++) {
                    result.put(resultSetMetaData.getColumnName(i), resultSet.getString(i));
                }
                resultList.add(result);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null) {
                closeConnection(conn);
            }
        }
        return resultList;
    }


    public static void main(String[] args) {
        List<Map<String, String>> list = getList("bruce.version_ck");
        System.err.println(list);

        List<String> tables = getTables("bruce");
        System.err.println(tables);
    }
}
