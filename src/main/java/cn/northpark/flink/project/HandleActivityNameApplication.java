package cn.northpark.flink.project;

import cn.northpark.flink.util.FlinkUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 *
 * 从kafka或者其他源读数据 关联mysql查询信息，返回Bean
 *
 *      u001,A1,2019-09-0210:10:11,1,北京市
 *      u002,A1,2019-09-0210:11:11,1,辽宁省
 *      u001,A1,2019-09-0210:11:11,2,北京市
 *      u001,A1,2019-09-0210:11:30,3,北京市
 *      u002,A1,2019-09-0210:12:11,2,辽宁省
 *      u003,A2,2019-09-0210:13:11,1,山东省
 *      u003,A22019-09-0210:13:20,2,山东省
 *      u003,A2,2019-09-0210:14:20,3,山东省
 *      u004,A1,2019-09-0210:15:20,1,北京市
 *      u004,A1,2019-09-0210:15:20,2,北京市
 *      u005,A1,2019-09-0210:15:20,1,河北省
 *      u001,A22019-09-0210:16:11,1,北京市
 *      u001,A2,2019-09-0210:16:11,2,北京市
 *      u002,A1,2019-09-0210:18:11,2,辽宁省
 *      u002,A1,2019-09-0210:19:11,3,辽宁省
 *  *  *
 *  *  *
 *  *  * id  name    last_update
 *  *  * A1  新人礼包 2019-10-15 11:36:36
 *  *  * A2  月末活动 2019-10-15 16:37:42
 *  *  * A3  周末活动 2019-10-15 11:44:23
 *  *  * A4  年度促销 2019-10-15 11:44:23
 *
 */
public class HandleActivityNameApplication {

    public static void main(String[] args) throws Exception {

        DataStream<String>  lines = FlinkUtilsV1.createKafkaStream(args,new SimpleStringSchema());

        SingleOutputStreamOperator<ActivityBean> beans = lines.map(new SourceToActivityBeanFunciton());

        beans.print();

        FlinkUtilsV1.getEnv().execute("HandleActivityNameApplication");

    }
}
