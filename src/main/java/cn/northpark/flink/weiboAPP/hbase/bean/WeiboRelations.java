package cn.northpark.flink.weiboAPP.hbase.bean;

import lombok.Data;

/**
 * @author bruce
 * @date 2022年06月26日 10:56:13
 */
@Data
public class WeiboRelations {
    private String user_id;

    /**
     * reply：回复
     * transLink:转发
     */
    private String rel_type;
    private String rel_user_id;

    /**
     * 1 : 被转发/被回复
     * 0 ： 主动转发/主动回复
     */
    private int by_type;
    private WeiboRelations(){};

    public WeiboRelations(String user_id, String rel_type, String rel_user_id ,int by_type) {
        this.user_id = user_id;
        this.rel_type = rel_type;
        this.rel_user_id = rel_user_id;
        this.by_type = by_type;
    }
}
