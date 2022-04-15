package cn.northpark.flink.join.orderAPP;

import java.util.Date;

/**
 * @author bruce
 * @date 2022年04月14日 10:42:54
 *
 * 数据预览
 * https://s1.ax1x.com/2022/04/15/L3sMKs.png
 * OrderDetail导入到Kafka的数据：
 * {"data":[{"id":"10","order_id":"29001","category_id":"2","sku":"20001","money":"1000.0","amount":"1",
 * "create_time":"2020-03-16 00:38:01","update_time":"2020-03-16 00:38:01"}],"database":"doit12",
 * "es":1584333481000,"id":4,"isDdl":false,"mysqlType":{"id":"bigint(20)","order_id":"bigint(20)",
 * "category_id":"int(11)","sku":"varchar(50)","money":"double","amount":"int(11)",
 * "create_time":"timestamp","update_time":"timestamp"},"old":null,"pkNames":["id"],"sql":"","sqlType":{"id
 * ":-5,"order_id":-5,"category_id":4,"sku":12,"money":8,"amount":4,"create_time":93,"update_time":93}
 * ,"table":"orderdetail","ts":1584333481600,"type":"INSERT"}
 */
public class OrderDetail {

    private Long id;
    private Long order_id;
    private int category_id;
    private String categoryName;
    private Long sku;
    private Double money;
    private int amount;
    private Date create_time;
    private Date update_time;

    //对数据库的操作类型：INSERT、UPDATE
    private String type;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getOrder_id() {
        return order_id;
    }

    public void setOrder_id(Long order_id) {
        this.order_id = order_id;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public Long getSku() {
        return sku;
    }

    public void setSku(Long sku) {
        this.sku = sku;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    @Override
    public String toString() {
        return "OrderDetail{" +
                "id=" + id +
                ", order_id=" + order_id +
                ", category_id=" + category_id +
                ", categoryName='" + categoryName + '\'' +
                ", sku=" + sku +
                ", money=" + money +
                ", amount=" + amount +
                ", create_time=" + create_time +
                ", update_time=" + update_time +
                ", type='" + type + '\'' +
                '}';
    }
}
