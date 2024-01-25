package cn.northpark.flink.bean;

/**
 * @author bruce
 * @date 2024年01月25日 13:44:13
 */
public class Message {
    private String msgType;
    private String body;

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

}
