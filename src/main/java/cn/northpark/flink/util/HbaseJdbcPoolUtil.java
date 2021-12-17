package cn.northpark.flink.util;

import lombok.Data;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Vector;

/**
 * 手动实现JDBC数据库连接池-HBase 特殊配置
 * @author bruce
 */
public class HbaseJdbcPoolUtil {

    /* 静态数据库配置实体对象，程序运行时加载进内存 */
    private static PoolConfig config = new PoolConfig();


    //初始化加载配置文件
    static {

        Properties prop = new Properties();


        try {

            prop.load(HbaseJdbcPoolUtil.class.getClassLoader().getResourceAsStream("phoenix.properties"));

            //获取配置文件信息传入config连接池配置对象

            config.setDriverName(prop.getProperty("driverClassName"));

            config.setUrl(prop.getProperty("jdbcUrl"));


            //反射加载这个驱动（使用的是JDBC的驱动加载方式）

            Class.forName(config.getDriverName());

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();

        }

    }

    private static ConnectionPool connPool = new ConnectionPool(config);

    public static Connection getConnection() throws SQLException {
        return connPool.getConnection();
    }

    public static Connection getConnection(boolean autoCommit) throws SQLException {
        Connection connection = connPool.getConnection();
        connection.setAutoCommit(autoCommit);
        return connection;
    }

    public static Connection getCurrentConnection() {
        return connPool.getCurrentConnection();
    }

    public static void closeConnection(Connection conn) {
        connPool.releaseConnection(conn);
    }

    public static void commit(Connection conn) {
        try {
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        finally {
//            connPool.releaseConnection(conn);
//        }
    }

    public static void rollBack(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        finally {
//            connPool.releaseConnection(conn);
//        }
    }

    /**
     * 连接池配置类
     * <p>
     * 线程安全
     * <p>
     * 有空闲连接的数量
     * <p>
     * 有正在使用的连接数量
     */
    @Data
    public static class PoolConfig {

        private String driverName;//数据库的驱动类

        private String url;//数据库的连接地址

        private String userName;//数据库用户名

        private String password;//数据库密码

        private int minConn = 5;//空闲集合中最少连接数

        private int maxConn = 10;//空闲集合最多的连接数

        private int initConn = 50;//初始连接数

        private int maxActiveConn = 200;//整个连接池（数据库）允许的最大连接数

        private int waitTime = 1000;//单位毫秒，连接数不够时，线程等待的时间

        private boolean isCheck = false;//数据库连接池是否启用自检机制（间隔一段时间检测连接池状态）

        private long checkPeriod = 1000 * 30;//自检周期

    }


    /**
     * 数据库连接池对象（根据配置创建对应连接池）
     */
    public static class ConnectionPool {
        private PoolConfig config;//连接池的配置对象
        private int count;//记录连接池的连接数
        private boolean isActive;//连接池是否被激活
        //空闲连接集合
        private Vector<Connection> freeConn = new Vector<Connection>();

        //正在使用的连接集合
        private Vector<Connection> useConn = new Vector<Connection>();


        //同一个线程无论请求多少次都使用同一个连接（使用ThreadLocal确保）

        //每一个线程都私有一个连接
        private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();

        public ConnectionPool(PoolConfig config) {
            this.config = config;
        }

        public void init() {
            for (int i = 0; i < config.getInitConn(); i++) {  //建立初始连接
                //获取连接对象
                Connection conn;
                try {
                    conn = getNewConnection();
                    freeConn.add(conn);
                    count++;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                isActive = true;//连接池激活
            }
        }


        private synchronized Connection getNewConnection() throws SQLException {
            Connection conn = null;
            Properties props = new Properties();
            props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
            props.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");

            conn = DriverManager.getConnection(config.getUrl(),props);
            return conn;
        }


        /**
         *  从连接池获取连接
         *  
         * @return
         * @throws SQLException
         */
        public synchronized Connection getConnection() throws SQLException {
            Connection conn = null;

            //当前连接总数小于配置的最大连接数才去获取

            if (count < config.getMaxActiveConn()) {
                //空闲集合中有连接数
                if (freeConn.size() > 0) {
                    conn = freeConn.get(0);//从空闲集合中取出
                    freeConn.remove(0);//移除该连接

                } else {
                    conn = getNewConnection();//拿到新连接
                    count++;
                }


                if (isEnable(conn)) {
                    useConn.add(conn);//添加到已经使用的连接
                } else {
                    count--;
                    conn = getConnection();//递归调用到可用的连接
                }
            } else {  //当达到最大连接数，只能阻塞等待

                try {
                    wait(config.getWaitTime());//线程睡眠了一段时间
                    conn = getConnection();//递归调用
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }
            //将获取的conn设置到本地变量ThreadLocal

            threadLocal.set(conn);

            return conn;

        }

        /**
         * 把用完的连接放回连接池集合Vector中
         *
         * @param conn
         */
        public synchronized void releaseConnection(Connection conn) {
            if (isEnable(conn)) {
                if (freeConn.size() < config.getMaxConn()) {  //空闲连接数没有达到最大
                    freeConn.add(conn);//放回集合
                } else {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

            }


            useConn.remove(conn);

            count--;

            threadLocal.remove();

            notifyAll();//放回连接池后说明有连接可用，唤醒阻塞的线程获取连接
        }

        /**
         * 获取当前线程的本地变量连接
         *
         * @return
         */
        public Connection getCurrentConnection() {
            return threadLocal.get();
        }

        /**
         * 判断该连接是否可用
         *
         * @param conn
         * @return
         */
        private boolean isEnable(Connection conn) {
            if (conn == null) {
                return false;
            }

            try {
                if (conn.isClosed()) {
                    return false;
                }
            } catch (SQLException e) {
                return false;
            }
            return true;
        }


    }

}
