package cn.northpark.flink.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 * 普通方式获取hbase连接和操作
 * @author bruce
 * @date 2022年6月26日
 */
@Slf4j
public class PhoenixUtilV2 {
    /**
     * 配置文件
     */
    private static transient Properties p = new Properties();

    static {
        InputStream inputStream = null;
        try {
            // java应用 读取配置文件
            inputStream = PhoenixUtilV2.class.getClassLoader().getResourceAsStream("phoenix.properties");
            p.load(inputStream);
            p.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
            p.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
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

    }


    private PhoenixUtilV2() {
    }

    /**
     * 通过数据源获取连接
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection()  {
        try {
            String driverClassName = p.getProperty("driverClassName");
            String jdbcUrl = p.getProperty("jdbcUrl");
            Class.forName(driverClassName);
            Connection conn = DriverManager.getConnection(jdbcUrl,p);
            return conn;
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
     * 创建表
     */
    public static void createTable(String sql) {
        Connection conn = getConnection();
        try {
            PreparedStatement sta = conn.prepareStatement(sql);
            sta.execute();
            System.out.println("创建成功...");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                PhoenixUtilV2.closeConnection(conn);
            }
        }

    }
    /**
     * 插入数据
     */
    public static void insertData(String sql) {
        Connection conn = getConnection();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            String msg = ps.executeUpdate() >0 ? "插入成功..."
                    :"插入失败...";
            conn.commit();
            System.out.println(msg);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                PhoenixUtilV2.closeConnection(conn);
            }
        }

    }

    /**
     * 删除数据
     */
    public static void delData(String sql) {
        Connection conn = getConnection();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            String msg = ps.executeUpdate() > 0 ? "删除成功..."
                    : "删除失败...";
            conn.commit();
            System.out.println(msg);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                PhoenixUtilV2.closeConnection(conn);
            }
        }

    }

    /**
     * 删除数据 --根据参数
     */
    public static void delData(String sql,Object... para) {
        Connection conn = getConnection();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            for(int i = 0;i < para.length;i++){
                ps.setObject(i + 1, para[i]);
            }
            String msg = ps.executeUpdate() > 0 ? "删除成功..."
                    : "删除失败...";
            conn.commit();
            System.out.println(msg);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                PhoenixUtilV2.closeConnection(conn);
            }
        }

    }

    /**
     * 删除表
     */
    public static void dropTable(String sql) {
        Connection conn = getConnection();
        //String sql = "drop table user";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute();
            System.out.println("删除表成功...");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                PhoenixUtilV2.closeConnection(conn);
            }
        }
    }

    /**
     * 获取Phoenix中的表(系统表除外)
     */
    public static List<String> getTables() {
        Connection conn = getConnection();
        List<String> tables = new ArrayList<>();
        try{
            DatabaseMetaData metaData = conn.getMetaData();
            String[] types = {"TABLE"}; //"SYSTEM TABLE"
            ResultSet resultSet = metaData.getTables(null, null, null, types);
            while (resultSet.next()) {
                tables.add(resultSet.getString("TABLE_NAME"));
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null) {
                PhoenixUtilV2.closeConnection(conn);
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
                PhoenixUtilV2.closeConnection(conn);
            }
        }
        return resultList;
    }

    /**
     * 查询sql
     */
    public static List<Map<String, String>> queryList(String sql,Object... para)  {
        Connection conn = getConnection();
        List<Map<String, String>> resultList = new ArrayList<>();
        try{
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            for(int i = 0;i < para.length;i++){
                preparedStatement.setObject(i + 1, para[i]);
            }
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
                PhoenixUtilV2.closeConnection(conn);
            }
        }
        return resultList;
    }


    public static void main(String[] args) {
//        List<Map<String, String>> us_population = getList("\"stt\".URL_STT_20211217");
//        System.err.println(us_population);
        List<Map<String, String>> us_population2 = queryList("select * from \"stt\".URL_STT_20211217");
        System.err.println(us_population2);
    }
}