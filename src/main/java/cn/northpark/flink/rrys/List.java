package cn.northpark.flink.rrys;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class List {

    /**
     * "HR-HDTV",
     * "MP4",
     * "APP"
     */
    @Expose
    private java.util.List<String> formats;
    @Expose
    private Items items;
    /**
     * "season_num": "0",
     * 		"season_cn": "正片",
     */
    @SerializedName("season_cn")
    private String seasonCn;
    @SerializedName("season_num")
    private String seasonNum;

    public java.util.List<String> getFormats() {
        return formats;
    }

    public void setFormats(java.util.List<String> formats) {
        this.formats = formats;
    }

    public Items getItems() {
        return items;
    }

    public void setItems(Items items) {
        this.items = items;
    }

    public String getSeasonCn() {
        return seasonCn;
    }

    public void setSeasonCn(String seasonCn) {
        this.seasonCn = seasonCn;
    }

    public String getSeasonNum() {
        return seasonNum;
    }

    public void setSeasonNum(String seasonNum) {
        this.seasonNum = seasonNum;
    }

}
