
package cn.northpark.flink.money.bean;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import javax.annotation.Generated;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class Lib {

    @Expose
    private String $lib;
    @SerializedName("$lib_method")
    private String $libMethod;
    @SerializedName("$lib_version")
    private String $libVersion;

    public String get$lib() {
        return $lib;
    }

    public void set$lib(String $lib) {
        this.$lib = $lib;
    }

    public String get$libMethod() {
        return $libMethod;
    }

    public void set$libMethod(String $libMethod) {
        this.$libMethod = $libMethod;
    }

    public String get$libVersion() {
        return $libVersion;
    }

    public void set$libVersion(String $libVersion) {
        this.$libVersion = $libVersion;
    }

}
