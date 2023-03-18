package cn.northpark.flink.util;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;


/**
 * JDBC辅助组件:
 * 可以保证在大数据组件中调用时保证线程安全
 * @author bruce
 *
 */
public class JDBCHelperV2 {
	private ThreadLocal<Connection> connThreadLocal = new ThreadLocal<>();

	private JDBCHelperV2() {
	}

	public static JDBCHelperV2 getInstance() {
		return Singleton.INSTANCE.getInstance();
	}

	private enum Singleton {
		INSTANCE;

		private JDBCHelperV2 singleton;

		Singleton() {
			singleton = new JDBCHelperV2();
		}

		public JDBCHelperV2 getInstance() {
			return singleton;
		}
	}

	public Connection getConnection() {
		Connection conn = connThreadLocal.get();
		if (conn == null) {
			// Create a new connection and set it to the thread local variable
			// You need to replace the connection string, username, and password with your own
			String url = "jdbc:mysql://localhost:3306/trip";
			String username = "root";
			String password = "123456";
			try {
				conn = DriverManager.getConnection(url, username, password);
				connThreadLocal.set(conn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return conn;
	}

	public void closeConnection() {
		Connection conn = connThreadLocal.get();
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				connThreadLocal.remove();
			}
		}
	}



	// The rest of the methods (executeUpdate, genCompleteSQL, executeQuery, executeBatch, and QueryCallback)
	// should be changed from static to instance methods
	// You should also remove the Connection conn parameter from the methods and use getConnection() method instead
	/**
	 * 第五步：开发增删改查的方法
	 * 1、执行增删改SQL语句的方法
	 * 2、执行查询SQL语句的方法
	 * 3、批量执行SQL语句的方法
	 * 执行增删改SQL语句，返回影响的行数
	 * @param sql
	 * @param params
	 * @return 影响的行数
	 */
	public int executeUpdate(String sql, Object... params) {
		Connection conn = getConnection();
		int rtn = 0;
		PreparedStatement pstmt = null;

		try {
			conn.setAutoCommit(false);

			pstmt = conn.prepareStatement(sql);

			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);
				}
			}

			// 拼接完整的 SQL 语句，并打印出来
			genCompleteSQL(pstmt, params);

			rtn = pstmt.executeUpdate();

			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			closeConnection();
		}

		return rtn;
	}

	/**
	 * 拼接完整的 SQL 语句，并打印出来
	 * @param pstmt
	 * @param params
	 */
	private void genCompleteSQL(PreparedStatement pstmt, Object[] params) {
		// 拼接完整的 SQL 语句，并打印出来
		String completeSql = pstmt.toString().substring(pstmt.toString().indexOf(":") + 2);
		for (Object param : params) {
			completeSql = completeSql.replaceFirst("\\?", param.toString());
		}
		System.out.println("Complete SQL: " + completeSql);
	}

	/**
	 * 执行查询SQL语句
	 * @param sql
	 * @param params
	 * @param callback
	 */
	public void executeQuery(String sql, JDBCHelperV2.QueryCallback callback , Object... params) {
		Connection conn = getConnection();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			pstmt = conn.prepareStatement(sql);

			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);
				}
			}
			// 拼接完整的 SQL 语句，并打印出来
			genCompleteSQL(pstmt, params);

			rs = pstmt.executeQuery();

			callback.process(rs);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			closeConnection();
		}
	}

	/**
	 * 批量执行SQL语句
	 *
	 * 批量执行SQL语句，是JDBC中的一个高级功能
	 * 默认情况下，每次执行一条SQL语句，就会通过网络连接，向MySQL发送一次请求
	 *
	 * 但是，如果在短时间内要执行多条结构完全一模一样的SQL，只是参数不同
	 * 虽然使用PreparedStatement这种方式，可以只编译一次SQL，提高性能，但是，还是对于每次SQL
	 * 都要向MySQL发送一次网络请求
	 *
	 * 可以通过批量执行SQL语句的功能优化这个性能
	 * 一次性通过PreparedStatement发送多条SQL语句，比如100条、1000条，甚至上万条
	 * 执行的时候，也仅仅编译一次就可以
	 * 这种批量执行SQL语句的方式，可以大大提升性能
	 *
	 * @param sql
	 * @param paramsList
	 * @return 每条SQL语句影响的行数
	 */
	public  int[] executeBatch(String sql, List<Object[]> paramsList) {
		Connection conn = getConnection();
		int[] rtn = null;
		PreparedStatement pstmt = null;

		try {
			// 第一步：使用Connection对象，取消自动提交
			conn.setAutoCommit(false);

			pstmt = conn.prepareStatement(sql);

			// 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
			if(paramsList != null && paramsList.size() > 0) {
				for(Object[] params : paramsList) {
					for(int i = 0; i < params.length; i++) {
						pstmt.setObject(i + 1, params[i]);
					}
					pstmt.addBatch();
				}
			}

			// 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
			rtn = pstmt.executeBatch();

			// 最后一步：使用Connection对象，提交批量的SQL语句
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			closeConnection();
		}

		return rtn;
	}

	/**
	 * 静态内部类：查询回调接口
	 * @author Administrator
	 *
	 */
	public interface QueryCallback {

		/**
		 * 处理查询结果
		 * @param rs
		 * @throws Exception
		 */
		void process(ResultSet rs) throws Exception;

	}

}
