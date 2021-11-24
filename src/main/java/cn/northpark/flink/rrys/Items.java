package cn.northpark.flink.rrys;

import java.util.List;
import javax.annotation.Generated;
import com.google.gson.annotations.SerializedName;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class Items {

    @SerializedName("cn.northpark.flink.rrys.FormatObj")
    private List<FormatObj> formatObjs;

    public List<FormatObj> getRMVB() {
        return formatObjs;
    }

    public void setRMVB(List<FormatObj> rMVB) {
        this.formatObjs = rMVB;
    }

}
