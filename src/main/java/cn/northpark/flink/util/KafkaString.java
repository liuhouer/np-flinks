package cn.northpark.flink.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.ResourceBundle;

/**
 * @author zhangyang
 * @date 2021年04月29日 15:36:25
 */
@Slf4j
public class KafkaString {

    /**
     * 构建基本的kafka属性返回
     * @return
     */
    public synchronized static Properties buildBasicKafkaProperty() {
        ResourceBundle bundle = ResourceBundle.getBundle("kafka");
        if(bundle==null){
            throw new IllegalArgumentException("找不到kafka.properties!");
        }
        String kafkaBootstrapServers = bundle.getString("bootstrap.servers");
        String groupID = bundle.getString("group.id");
        String keyd = bundle.getString("key.deserializer");
        String keys = bundle.getString("key.serializer");
        String valued = bundle.getString("value.deserializer");
        String values = bundle.getString("value.serializer");

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", kafkaBootstrapServers);
        kafkaProperties.put("group.id", groupID);
        kafkaProperties.put("key.deserializer", keyd);
        kafkaProperties.put("value.deserializer", valued);
        kafkaProperties.put("key.serializer", keys);
        kafkaProperties.put("value.serializer", values);


        return kafkaProperties;
    }

    /**
     * 发送String序列化的kafka消息
     * @param props
     * @param topic
     * @param msg
     */
    public synchronized static void sendKafkaString(Properties props , String topic , String msg) {

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            log.info("kafka Send :["+topic+"]"+"--->"+msg);
        	producer.send(new ProducerRecord<String, String>(topic, msg));
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }


    }
    
    public static void main(String[] args) {
    	Properties properties = KafkaString.buildBasicKafkaProperty();
//    	String msg = "[np_web][INFO] [2022-04-06 17:58:45] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{\"class_method\":\"cn.northpark.action.MoviesAction.tag_list_page\",\"cookieMap\":{\"UA\":\"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"},\"ip\":\"172.17.0.2\",\"method\":\"GET\",\"url\":\"http://northpark.cn/movies/tag/juqing/page/1272\"}";
//		KafkaString.sendKafkaString(properties,"flink000",msg);
        while (true){
            String msg = "02\t02\t01\t68773\t1653185731000\t京F99200\t29.11";
            KafkaString.sendKafkaString(properties,"flink_traffic2",msg);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

	}
    

}
