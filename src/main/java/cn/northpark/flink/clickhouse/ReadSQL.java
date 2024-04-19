package cn.northpark.flink.clickhouse;

import cn.northpark.flink.util.KafkaString;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * @author bruce
 * @date 2024年04月18日 17:31:29
 */
public class ReadSQL {
    public static void main(String[] args) throws Exception {
        String sqlFilePath = "C:\\Users\\Bruce\\Desktop\\drg_pay.sql"; // SQL 文件路径
        try (BufferedReader reader = new BufferedReader(new FileReader(sqlFilePath))) {

            String line;
            StringBuilder batch = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                try {
                    batch.append(line.trim());

                    if (line.endsWith(";")) {
                        // 发送到 Kafka
                        KafkaString.sendKafkaString(KafkaString.buildBasicKafkaProperty(),"drg_pay",batch.toString());
                        batch.setLength(0); // 清空批次
                    }
                }catch (Exception e){

                }

            }

        }
    }
}
