package cn.northpark.flink.project;

/**
 * 从kafka或者其他源读数据 关联mysql查询信息，返回Bean
 *
 * u001,A1,2019-09-02 10:10:11,1 ,北京市
 * u002 ,A1,2019-09-0210:11:11,1,辽宁省
 * u001,A1,2019-09-02 10:11:11,2 ,北京市
 * u001,A1,2019-09-02 10:11:30,3, 北京市
 * u002 ,A1,2019-09-0210:12:11,2 ,辽宁省
 * u003,A2,2019-09-02 10:13:11,1, 山东省
 * u003 ,A22019-09-0210:13:20,2, 山东省
 * u003,A2 ,2019-09-0210:14:20,3, 山东省
 * u004,A1,2019-09-02 10:15:20,1, 北京市
 * u004, A1,2019-09-0210:15:20,2,北京市
 * u005,A1,2019-09-02 10:15:20, 1 ,河北省
 * u001,A2 2019-09-0210:16:11, 1 ,北京市
 * u001,A2, 2019-09-0210:16:11,2 ,北京市
 * u002 ,A1, 2019-09-0210:18:11,2,辽宁省
 * u002 ,A1,2019-09-02 10:19:11,3 ,辽宁省
 *
 *
 * id  name    last_update
 * A1  新人礼包 2019-10-15 11:36:36
 * A2  月末活动 2019-10-15 16:37:42
 * A3  周末活动 2019-10-15 11:44:23
 * A4  年度促销 2019-10-15 11:44:23
 *
 *
 */
public class ActivityBean {
    public String uid;
    public String aid;
    public String activityName;
    public String time ;
    public int eventType;
    public String province;

    public ActivityBean(String uid, String aid, String activityName, String time, int eventType, String province) {
        this.uid = uid;
        this.aid = aid;
        this.activityName = activityName;
        this.time = time;
        this.eventType = eventType;
        this.province = province;
    }

    public static ActivityBean of(String uid, String aid, String activityName, String time, int eventType, String province){
        return new ActivityBean(uid,aid,activityName,time,eventType,province);
    }

    @Override
    public String toString() {
        return "ActivityBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", activityName='" + activityName + '\'' +
                ", time='" + time + '\'' +
                ", eventType=" + eventType +
                ", province='" + province + '\'' +
                '}';
    }
}
