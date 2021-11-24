package cn.northpark.flink.rrys;

import java.util.List;
import javax.annotation.Generated;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class Info {

    //"info": {
    //			"id": 32294,
    //			"cnname": "圣诞树3",
    //			"enname": "Yolki 3",
    //			"aliasname": "Ёлки 3",
    //			"channel": "movie",
    //			"channel_cn": "电影",
    //			"area": "俄罗斯",
    //			"show_type": "",
    //			"expire": "1610398029",
    //			"views": 0,
    //			"year": [
    //				2013
    //			]
    //		},

    @Expose
    private String aliasname;
    @Expose
    private String area;
    @Expose
    private String channel;
    @SerializedName("channel_cn")
    private String channelCn;
    @Expose
    private String cnname;
    @Expose
    private String enname;
    @Expose
    private String expire;
    @Expose
    private Long id;
    @SerializedName("show_type")
    private String showType;
    @Expose
    private Long views;
    @Expose
    private List<Long> year;

    public String getAliasname() {
        return aliasname;
    }

    public void setAliasname(String aliasname) {
        this.aliasname = aliasname;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getChannelCn() {
        return channelCn;
    }

    public void setChannelCn(String channelCn) {
        this.channelCn = channelCn;
    }

    public String getCnname() {
        return cnname;
    }

    public void setCnname(String cnname) {
        this.cnname = cnname;
    }

    public String getEnname() {
        return enname;
    }

    public void setEnname(String enname) {
        this.enname = enname;
    }

    public String getExpire() {
        return expire;
    }

    public void setExpire(String expire) {
        this.expire = expire;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getShowType() {
        return showType;
    }

    public void setShowType(String showType) {
        this.showType = showType;
    }

    public Long getViews() {
        return views;
    }

    public void setViews(Long views) {
        this.views = views;
    }

    public List<Long> getYear() {
        return year;
    }

    public void setYear(List<Long> year) {
        this.year = year;
    }

}
