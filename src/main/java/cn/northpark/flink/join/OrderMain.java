package cn.northpark.flink.join;

import java.util.Date;

/**
 * @author bruce
 * @date 2022年04月14日 10:43:31
 */
public class OrderMain {
    private Long oid;
    private Date create_time;
    private Double total_money;
    private int status;
    private Date update_time;
    private String province;
    private String city;
    //对数据库的操作类型：INSERT、UPDATE
    private String type;

    public Long getOid() {
        return oid;
    }

    public void setOid(Long oid) {
        this.oid = oid;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Double getTotal_money() {
        return total_money;
    }

    public void setTotal_money(Double total_money) {
        this.total_money = total_money;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "OrderMain{" +
                "oid=" + oid +
                ", create_time=" + create_time +
                ", total_money=" + total_money +
                ", status=" + status +
                ", update_time=" + update_time +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
