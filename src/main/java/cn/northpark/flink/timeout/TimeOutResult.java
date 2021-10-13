package cn.northpark.flink.timeout;

import java.io.Serializable;

/**
 * @author zhangyang
 * @date 2020年12月15日 15:15:35
 */
public class TimeOutResult implements Serializable {

    public TimeOutResult() {
    }

    public String queueName;
    public String primaryKey;
    public String resultMsg;
    public String status;


    public TimeOutResult(String queueName, String primaryKey, String resultMsg,String status) {
        this.queueName = queueName;
        this.primaryKey = primaryKey;
        this.resultMsg = resultMsg;
        this.status = status;
    }

    @Override
    public String toString() {
        return "TimeOutResult{" +
                "queueName='" + queueName + '\'' +
                ", primaryKey='" + primaryKey + '\'' +
                ", resultMsg='" + resultMsg + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
