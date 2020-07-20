
package cn.northpark.flink.table_sql_api.stream.sql.udf;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class UserBrowseLog {

    @Expose
    private String eventTime;
    @Expose
    private String eventType;
    @Expose
    private String productID;
    @Expose
    private int productPrice;
    @Expose
    private String userID;
    @Expose
    private Long eventTimeTimestamp;

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public int getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(int productPrice) {
        this.productPrice = productPrice;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public Long getEventTimeTimestamp() {
        return eventTimeTimestamp;
    }

    public void setEventTimeTimestamp(Long eventTimeTimestamp) {
        this.eventTimeTimestamp = eventTimeTimestamp;
    }

}
