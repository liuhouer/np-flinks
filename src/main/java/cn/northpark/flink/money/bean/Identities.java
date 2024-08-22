
package cn.northpark.flink.money.bean;

import com.google.gson.annotations.SerializedName;

import javax.annotation.Generated;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class Identities {

    @SerializedName("$identity_anonymous_id")
    private String $identityAnonymousId;
    @SerializedName("$identity_mp_id")
    private String $identityMpId;

    public String get$identityAnonymousId() {
        return $identityAnonymousId;
    }

    public void set$identityAnonymousId(String $identityAnonymousId) {
        this.$identityAnonymousId = $identityAnonymousId;
    }

    public String get$identityMpId() {
        return $identityMpId;
    }

    public void set$identityMpId(String $identityMpId) {
        this.$identityMpId = $identityMpId;
    }

}
