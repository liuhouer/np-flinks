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

/**
 * 抽象定义
 */

interface PhoenixTaskInterface {
    void open();
    void close();
    void execute(String sql);
    int executeUpdate(String sql);
    int executeUpdate(String sql,Object... para);
    List<Map<String, String>>  sqlQuery(String sql);
    List<Map<String, String>>  sqlQuery(String sql,Object... para);
}

abstract class PhoenixTask implements PhoenixTaskInterface {


    public transient Connection conn = null;
    @Override
    public final void open() {
        conn = PhoenixUtilV3.getConnection();
    }

    @Override
    public final void close() {
        if(Objects.nonNull(conn)){
            PhoenixUtilV3.closeConnection(conn);
        }
    }

    @Override
    public final void execute(String sql) {
        open();
        outExecute(sql);
        close();

    }

    @Override
    public final int executeUpdate(String sql) {
        open();
        int i = outExecuteUpdate(sql);
        close();

        return i;
    }

    @Override
    public int executeUpdate(String sql, Object... para) {
        open();
        int i = outExecuteUpdate(sql,para);
        close();

        return i;
    }

    @Override
    public final List<Map<String, String>> sqlQuery(String sql) {
        open();
        List<Map<String, String>> list = outSqlQuery(sql);
        close();
        return list;
    }

    @Override
    public final List<Map<String, String>> sqlQuery(String sql, Object... para) {
        open();
        List<Map<String, String>> list = outSqlQuery(sql,para);
        close();
        return list;
    }


    protected abstract void outExecute(String sql);
    protected abstract List<Map<String, String>> outSqlQuery(String sql, Object... para);
    protected abstract List<Map<String, String>> outSqlQuery(String sql);
    protected abstract int outExecuteUpdate(String sql);
    protected abstract int outExecuteUpdate(String sql, Object... para);
}


@Slf4j
public class PhoenixUtilV3 extends PhoenixTask{



    /**
     * 配置文件
     */
    private static transient Properties p = new Properties();

    static {
        InputStream inputStream = null;
        try {
            // java应用 读取配置文件
            inputStream = PhoenixUtilV3.class.getClassLoader().getResourceAsStream("phoenix.properties");
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

    /**
     * 通过数据源获取连接
     *
     * @return
     * @throws SQLException
     */
    protected static Connection getConnection()  {
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

    protected static void closeConnection(Connection conn){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
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
                PhoenixUtilV3.closeConnection(conn);
            }
        }
        return tables;
    }


    @Override
    @Deprecated
    protected void outExecute(String sql) {
        try {
            PreparedStatement sta = conn.prepareStatement(sql);
            sta.execute();
            System.out.println("创建成功...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    @Deprecated
    protected List<Map<String, String>> outSqlQuery(String sql, Object... para) {
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
        }
        return resultList;
    }

    @Override
    @Deprecated
    protected List<Map<String, String>> outSqlQuery(String sql) {
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
        }
        return resultList;
    }

    @Override
    @Deprecated
    protected int outExecuteUpdate(String sql) {
        int rs = 0;
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            rs = ps.executeUpdate();
            String msg = rs > 0 ? "删除成功..."
                    : "删除失败...";
            conn.commit();
            System.out.println(msg);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    @Override
    @Deprecated
    protected int outExecuteUpdate(String sql, Object... para) {
        int rs = 0;
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            for(int i = 0;i < para.length;i++){
                ps.setObject(i + 1, para[i]);
            }
            rs = ps.executeUpdate();
            String msg = rs > 0 ? "执行成功..."
                    : "执行失败...";
            conn.commit();
            System.out.println(msg);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rs;
    }



    public static void main(String[] args) {
//        List<Map<String, String>> us_population = getList("\"stt\".URL_STT_20211217");
//        System.err.println(us_population);

        PhoenixUtilV3 task = new PhoenixUtilV3();

        List<Map<String, String>> transLink = task.sqlQuery("SELECT ID, USER_ID, REL_TYPE, REL_USER_ID, BY_TYPE " +
                "FROM \"stt\".T_WEIBO_RELATIONS_V2  where REL_TYPE = 'transLink'");


        List<Map<String, String>> maps = task.outSqlQuery("SELECT ID, USER_ID, REL_TYPE, REL_USER_ID, BY_TYPE " +
                "FROM \"stt\".T_WEIBO_RELATIONS_V2  where REL_TYPE = 'transLink'");
        System.err.println(transLink);
        System.err.println(maps);


    }

}
