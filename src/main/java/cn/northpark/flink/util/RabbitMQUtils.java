                    
package cn.northpark.flink.util;

import cn.northpark.flink.MQ.MerchantDaySta;
import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;

/**
 * 操作rabbitmq队列工具类,支持集群自动重连
 * 
 * @author bruce
 *
 */
@Slf4j
public class RabbitMQUtils {
	
	final static String CHARSET_UTF8 = "UTF-8";
	static Channel channel = null;

	/**
	 * 发送消息到rabbitmq
	 * 
	 * @param queueName
	 *            队列名
	 * @param Message
	 *            消息
	 */
	public static void Send(String queueName, String Message) {
		try {
			if(channel==null)
			{
				Connection connection = RabbitMQConFactory.getConnection("common");
				channel = connection.createChannel();
			}
			channel.queueDeclare(queueName, true, false, false, null);
			channel.basicPublish("", queueName, null, Message.getBytes(CHARSET_UTF8));
			log.info(" Sent '" + Message + "' SUCCESS!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws InterruptedException {

		MerchantDaySta bean = new MerchantDaySta("1001",20.55);
//		MerchantDaySta bean2 = new MerchantDaySta("1002",80.55);
//		MerchantDaySta bean3 = new MerchantDaySta("1002",70.55);

		RabbitMQUtils.Send("flink_amount_queue", JSON.toJSONString(bean));
//		RabbitMqUtils.Send("flink_amount_queue", JSON.toJSONString(bean2));
//		RabbitMqUtils.Send("flink_amount_queue", JSON.toJSONString(bean3));
	}
}
