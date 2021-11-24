package cn.northpark.flink.rrys;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class Data {

    /**
     * "info": {
     * 			"id": 32294,
     * 			"cnname": "圣诞树3",
     * 			"enname": "Yolki 3",
     * 			"aliasname": "Ёлки 3",
     * 			"channel": "movie",
     * 			"channel_cn": "电影",
     * 			"area": "俄罗斯",
     * 			"show_type": "",
     * 			"expire": "1610398029",
     * 			"views": 0,
     * 			"year": [
     * 				2013
     * 			]
     *                },
     */
    @Expose
    private Info info;
    @Expose
    private java.util.List<List> list;

    public Info getInfo() {
        return info;
    }

    public void setInfo(Info info) {
        this.info = info;
    }

    public java.util.List<List> getList() {
        return list;
    }

    public void setList(java.util.List<List> list) {
        this.list = list;
    }

}
