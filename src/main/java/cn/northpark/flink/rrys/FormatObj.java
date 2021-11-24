package cn.northpark.flink.rrys;

import java.util.List;
import javax.annotation.Generated;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class FormatObj {

    @Expose
    private String dateline;
    @Expose
    private String episode;
    @Expose
    private List<File> files;
    @Expose
    private String itemid;
    @Expose
    private String name;
    @Expose
    private String size;
    @SerializedName("yyets_trans")
    private Long yyetsTrans;

    public String getDateline() {
        return dateline;
    }

    public void setDateline(String dateline) {
        this.dateline = dateline;
    }

    public String getEpisode() {
        return episode;
    }

    public void setEpisode(String episode) {
        this.episode = episode;
    }

    public List<File> getFiles() {
        return files;
    }

    public void setFiles(List<File> files) {
        this.files = files;
    }

    public String getItemid() {
        return itemid;
    }

    public void setItemid(String itemid) {
        this.itemid = itemid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public Long getYyetsTrans() {
        return yyetsTrans;
    }

    public void setYyetsTrans(Long yyetsTrans) {
        this.yyetsTrans = yyetsTrans;
    }

}
