package cn.northpark.flink.weiboAPP.hbase;

import cn.northpark.flink.util.PhoenixUtilV2;
import cn.northpark.flink.weiboAPP.hbase.bean.WeiboRelations;
import cn.northpark.flink.weiboAPP.hbase.enums.RelType;

import java.util.UUID;

/**
 * @author bruce
 * @date 2022年06月26日 10:03:33
 */
public class AddMony {
    public static void main(String[] args) {

        String uid = UUID.randomUUID().toString().replace("-","");
        //添加1对多的转发关系
//              A -B1
//                -B2
//                -B3
        for (int i = 0; i < 20; i++) {

            String rel_uid_ = UUID.randomUUID().toString().replace("-","");
            WeiboRelations bean_by_ = new WeiboRelations(uid, RelType.TRANS_LINK,rel_uid_,1);
            addOne(bean_by_);
        }
    }

    /**
     * 添加一条转发/评论关系
     * @param bean_by
     */
    private static void addOne(WeiboRelations bean_by) {
        //根据被转发关系构造一条主动转发关系
        WeiboRelations bean_self = new WeiboRelations(bean_by.getRel_user_id(), bean_by.getRel_type(), bean_by.getUser_id(),bean_by.getBy_type()==1?0:1);

        //分别插入2条数据

        String insert_rel_sql =  "UPSERT INTO \"stt\".t_weibo_relations_v2 (ID,USER_ID,REL_TYPE,REL_USER_ID,BY_TYPE) " +
                " VALUES ( '" + UUID.randomUUID().toString()+"' ,'"+ bean_by.getUser_id()+"' ,'"+ bean_by.getRel_type()+"','"+ bean_by.getRel_user_id()+"',"+ bean_by.getBy_type()+" )";


        String insert_rel_sql2 =  "UPSERT INTO \"stt\".t_weibo_relations_v2 (ID,USER_ID,REL_TYPE,REL_USER_ID,BY_TYPE) " +
                " VALUES ( '" + UUID.randomUUID().toString()+"' ,'"+ bean_self.getUser_id()+"' ,'"+bean_self.getRel_type()+"','"+bean_self.getRel_user_id()+"',"+bean_self.getBy_type()+" )";


        PhoenixUtilV2.insertData(insert_rel_sql);
        PhoenixUtilV2.insertData(insert_rel_sql2);
    }
}
