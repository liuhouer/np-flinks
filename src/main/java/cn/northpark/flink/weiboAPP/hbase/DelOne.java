package cn.northpark.flink.weiboAPP.hbase;

import cn.northpark.flink.util.PhoenixUtilV2;
import cn.northpark.flink.weiboAPP.hbase.bean.WeiboRelations;
import cn.northpark.flink.weiboAPP.hbase.enums.RelType;

/**
 * @author bruce
 * @date 2022年06月26日 12:10:52
 */
public class DelOne {
    public static void main(String[] args) {
        //3.删除一条关系
        //构造A的b
        //8538d7f7b0724b518b33129672a38915	reply	4ef14c4e222d4a9c8110d1a435a24ea3	1
        WeiboRelations bean_by = new WeiboRelations("8538d7f7b0724b518b33129672a38915", RelType.REPLY,"4ef14c4e222d4a9c8110d1a435a24ea3",1);
        delOne(bean_by);
    }

    /**
     * 删除一条转发/评论关系
     */
    private static void delOne(WeiboRelations bean_by) {

        String del_one_sql = "delete from \"stt\".t_weibo_relations_v2 where user_id = ? and rel_type = ?  and rel_user_id = ? and by_type = ?";
        //删除A的b
        PhoenixUtilV2.delData(del_one_sql,
                bean_by.getUser_id(),
                bean_by.getRel_type(),
                bean_by.getRel_user_id(),
                bean_by.getBy_type());
        //删除b的A
        PhoenixUtilV2.delData(del_one_sql,
                bean_by.getRel_user_id(),
                bean_by.getRel_type(),
                bean_by.getUser_id(),
                bean_by.getBy_type()==1?0:1);
    }
}
