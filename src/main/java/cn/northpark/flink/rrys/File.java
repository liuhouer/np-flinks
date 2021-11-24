package cn.northpark.flink.rrys;

import javax.annotation.Generated;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class File {

    @Expose
    private String address;
    @Expose
    private String passwd;
    @Expose
    private String way;
    @SerializedName("way_cn")
    private String wayCn;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public String getWay() {
        return way;
    }

    public void setWay(String way) {
        this.way = way;
    }

    public String getWayCn() {
        return wayCn;
    }

    public void setWayCn(String wayCn) {
        this.wayCn = wayCn;
    }

}
