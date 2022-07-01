package cn.northpark.flink.vehicleAPP.task;

import cn.hutool.cron.CronUtil;
import cn.hutool.cron.task.Task;
import cn.northpark.flink.vehicleAPP.bean.GPSRealData;
import cn.northpark.flink.util.KafkaString;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * @author bruce
 * @date 2022年07月01日 15:11:06
 * 定时任务每分钟执行一次，从系统车辆上线集合的redis缓存中，根据score获取超过3分钟未上线的数据，生成一条消息发送到topic: json-vehicle-offline
 */
@Slf4j
public class HandleRedisCron {

    //每1分钟
//    private static String cronString = "0 0/1 * * * ? ";
    //每秒
    private static String cronString = "0/30 * * * * ? ";

    private static String scheduleId;
    final String REDIS_PREFIX = "FLINK_RDF_GPS_REAL_DATA";
    final Long  TIME_LIMIT = 3 * 60 * 1000L;

    final String OFFLINE_TOPIC = "json-vehicle-offline";

    public String execute() {
        String jobId = CronUtil.schedule(cronString, new Task() {
            @Override
            public void execute() {
                System.out.println("-------------------HandleRedisCron任务执行开始-------------------");
                System.out.println(new Date());

                //初始化
                Jedis jedis = new Jedis("node1", 6379, 5000);
                jedis.auth("123456");
                jedis.select(0);

                long nowTime = new Date().getTime();

                try{
                    //todo 改为  获取超过3分钟未上线的数据
                    //定时任务每分钟执行一次，从系统车辆上线集合的redis缓存中，根据score获取超过3分钟未上线的数据
                    Map<String, String> stringMap = jedis.hgetAll(REDIS_PREFIX);

                    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
                        System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                        GPSRealData gpsRealData = JSON.parseObject(entry.getValue(), GPSRealData.class);
                        if(Objects.nonNull(gpsRealData)){
                            Long sendTime = gpsRealData.getSendTime();
                            if(nowTime-sendTime>TIME_LIMIT){
                                //发送下线消息到生成一条消息发送到topic: json-vehicle-offline

                                KafkaString.sendKafkaString(KafkaString.buildBasicKafkaProperty(),OFFLINE_TOPIC,entry.getValue());
                            }
                        }
                    }
                }finally {
                    if(jedis!=null){
                        jedis.close();
                    }
                }


                System.out.println("-------------------HandleRedisCron任务执行结束-------------------");
            }
        });
        if (!StringUtils.isEmpty(jobId)) {
            scheduleId = jobId;
            return jobId;
        } else {
            return null;
        }
    }

    public void remove() {
        CronUtil.remove(scheduleId);
    }

    public static void main(String[] args) {
        HandleRedisCron m =  new HandleRedisCron();
        CronUtil.setMatchSecond(true);
        CronUtil.start();
        m.execute();
    }
}
