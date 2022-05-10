package cn.northpark.flink.bean;

import lombok.Data;

/**
 * @author bruce
 * @date 2022年05月08日 22:49:44
 */
@Data
public class Covid {

    //    'date':'日期','name':'名称','id':'编号','lastUpdateTime':'更新时间',
//            'today_confirm':'当日新增确诊','today_suspect':'当日新增疑似',
//            'today_heal':'当日新增治愈','today_dead':'当日新增死亡',
//            'today_severe':'当日新增重症','today_storeConfirm':'当日现存确诊', 'today_input':'当日输入',
//            'total_confirm':'累计确诊','total_suspect':'累计疑似','total_heal':'累计治愈',
//            'total_dead':'累计死亡','total_severe':'累计重症','total_input':'累计输入'
    private String id;
    private String lastUpdateTime;
    private String name;
    private String total_confirm;//累计确诊
    private String total_suspect;//累计疑似
    private String total_heal;//累计治愈
    private String total_dead;//累计死亡
    private String total_severe;//累计重症
    private String total_input;//累计输入

    private String total_newConfirm;//当日现存确诊
    private String total_newDead;//当日现存死亡
    private String total_newHeal;//当日现存治愈

    private String today_confirm;//当日新增确诊
    private String today_suspect;//当日新增疑似
    private String today_heal;//当日新增治愈
    private String today_dead;//当日新增死亡
    private String today_severe;//当日新增重症
    private String today_storeConfirm;//当日现存确诊

    private String store_confirm;//现存确诊
    private Double dead_rate;//病死率

    public Covid() {
    }

    public Covid(String id, String lastUpdateTime, String name, String total_confirm, String total_suspect, String total_heal, String total_dead, String total_severe, String total_input, String total_newConfirm, String total_newDead, String total_newHeal, String today_confirm, String today_suspect, String today_heal, String today_dead, String today_severe, String today_storeConfirm) {
        this.id = id;
        this.lastUpdateTime = lastUpdateTime;
        this.name = name;
        this.total_confirm = total_confirm;
        this.total_suspect = total_suspect;
        this.total_heal = total_heal;
        this.total_dead = total_dead;
        this.total_severe = total_severe;
        this.total_input = total_input;
        this.total_newConfirm = total_newConfirm;
        this.total_newDead = total_newDead;
        this.total_newHeal = total_newHeal;
        this.today_confirm = today_confirm;
        this.today_suspect = today_suspect;
        this.today_heal = today_heal;
        this.today_dead = today_dead;
        this.today_severe = today_severe;
        this.today_storeConfirm = today_storeConfirm;
    }

    @Override
    public String toString() {
        return "Covid{" +
                "id='" + id + '\'' +
                ", lastUpdateTime='" + lastUpdateTime + '\'' +
                ", name='" + name + '\'' +
                ", total_confirm=" + total_confirm +
                ", total_suspect=" + total_suspect +
                ", total_heal=" + total_heal +
                ", total_dead=" + total_dead +
                ", total_severe=" + total_severe +
                ", total_input=" + total_input +
                ", total_newConfirm=" + total_newConfirm +
                ", total_newDead=" + total_newDead +
                ", total_newHeal=" + total_newHeal +
                ", today_confirm=" + today_confirm +
                ", today_suspect=" + today_suspect +
                ", today_heal=" + today_heal +
                ", today_dead=" + today_dead +
                ", today_severe=" + today_severe +
                ", today_storeConfirm=" + today_storeConfirm +
                '}';
    }
}
