
package cn.northpark.flink.money.bean;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import javax.annotation.Generated;

@Generated("net.hexar.json2pojo")
@SuppressWarnings("unused")
public class EventMsg {

    @SerializedName("_flush_time")
    private Long _flushTime;
    @SerializedName("_track_id")
    private Long _trackId;
    @SerializedName("anonymous_id")
    private String anonymousId;
    @SerializedName("distinct_id")
    private String distinctId;
    @Expose
    private String event;
    @Expose
    private Identities identities;
    @Expose
    private Lib lib;
    @Expose
    private Properties properties;
    @Expose
    private Long time;
    @Expose
    private String type;

    public Long get_flushTime() {
        return _flushTime;
    }

    public void set_flushTime(Long _flushTime) {
        this._flushTime = _flushTime;
    }

    public Long get_trackId() {
        return _trackId;
    }

    public void set_trackId(Long _trackId) {
        this._trackId = _trackId;
    }

    public String getAnonymousId() {
        return anonymousId;
    }

    public void setAnonymousId(String anonymousId) {
        this.anonymousId = anonymousId;
    }

    public String getDistinctId() {
        return distinctId;
    }

    public void setDistinctId(String distinctId) {
        this.distinctId = distinctId;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Identities getIdentities() {
        return identities;
    }

    public void setIdentities(Identities identities) {
        this.identities = identities;
    }

    public Lib getLib() {
        return lib;
    }

    public void setLib(Lib lib) {
        this.lib = lib;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
