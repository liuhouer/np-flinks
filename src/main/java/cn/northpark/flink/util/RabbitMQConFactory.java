package cn.northpark.flink.util;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.*;

/**
 * 消息连接工厂，支持集群自动重连
 * 
 * @author bruce
 *
 */
public class RabbitMQConFactory {
	
	/**
	 * 缓存连接工厂,将建立的链接放入map缓存，为每个Storm的spout都建立独立的连接，其他用通用的。
	 */
	private static Map<String, Connection> connectionMap = new HashMap<String, Connection>();

	private static ConnectionFactory factory=new ConnectionFactory();
	private static List<Address> addrs=new ArrayList<Address>();
	static {

		ResourceBundle bundle = ResourceBundle.getBundle("config");
		if(bundle==null){
			throw new IllegalArgumentException("找不到config.properties!");
		}
		factory.setAutomaticRecoveryEnabled(true);
		factory.setUsername(bundle.getString("mq.user"));
		factory.setPassword(bundle.getString("mq.pass"));
		String address=bundle.getString("mq.host");
		int port=Integer.parseInt(bundle.getString("mq.port"));

		Address address1= new Address(address,port);
		addrs.add(address1);

//		String []addressArray=addresses.split(",");
//		for(int i=0;i<addressArray.length;i++)
//		{
//			Address address=new Address(addressArray[i].split(":")[0],Integer.valueOf(addressArray[i].split(":")[1]));
//			addrs.add(address);
//		}
	}
	/**
	 * 获取connectionName
	 * 新建或者从map中获取
	*/
	public static Connection getConnection(String connectionName) {
		if(connectionName==null)
		{
			return connectionMap.get("common");
		}
		if(connectionMap.get(connectionName)==null){
			try {
				Connection connection = factory.newConnection(addrs);
				connectionMap.put(connectionName, connection);
				return connection;
			} 
			catch (Exception e) {
				return null;
			}

	    }
		else
		{
			return connectionMap.get(connectionName);
		}
	 }
	/*
	 * 获取默认连接
	 */
	public static Connection getConnection() 
	{
		return getConnection("common");
	}
}
