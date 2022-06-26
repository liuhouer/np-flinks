package cn.northpark.flink.weiboAPP.hbase;

import cn.northpark.flink.util.PhoenixUtilV2;
import cn.northpark.flink.weiboAPP.hbase.bean.WeiboRelations;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author bruce
 * @date 2022年06月26日 10:03:33
 *
 * 下图是微博谣言话题中部分意见领袖结节关系图谱，请用同学们设
 * 计合适的表结构，用Hbase 来保存图内的关系；
 * 用java 代码分别实现以下功能
 * 1.
 * 增加一条关系
 * 2.
 * 3.
 * 删除一条关系
 * 删除一批关系
 */
public class CreateTableTest {

    public static void main(String[] args) {

        //1.建表
//        String t_weibo_relations_sql = "CREATE TABLE IF NOT EXISTS \"stt\".t_weibo_relations  (\n" +
//                "      user_id VARCHAR NOT NULL,\n" +
//                "      rel_type VARCHAR ,\n" +
//                "      rel_user_id VARCHAR \n" +
//                "      CONSTRAINT PK PRIMARY KEY (user_id)\n" +
//                ")";
//        PhoenixUtilV2.createTable(t_weibo_relations_sql);


        //2.增加1条关系
//        String uid = UUID.randomUUID().toString().replace("-","");
//        String rel_uid = UUID.randomUUID().toString().replace("-","");
//        WeiboRelations bean_by = new WeiboRelations(uid,RelType.REPLY,rel_uid,1);
//        addOne(bean_by);

        //3.删除一条关系
        //构造A的b
//        WeiboRelations bean_by = new WeiboRelations("3d687f69b0a64278afd7c0645f2efbf6",RelType.REPLY,"9c32a44523ca458da4c72fd48abbee2c",0);
//        delOne(bean_by);


        //4.删除一批关系
        //把 A的所有【被转发/被评论关系】删除
        //构造A的b
        String userid = "";
        delMany( userid);


    }

    private static void delMany(String userid) {
        //查询A的list
        List<Map<String, String>> mapList = PhoenixUtilV2.queryList("select \"stt\".t_weibo_relations where user_id = ? and by_type = 0");
        List<String> sel_list = mapList.stream().filter(t -> Objects.nonNull(t.get("rel_user_id"))).map(t -> t.get("rel_user_id").toString()).collect(Collectors.toList());

        //删除A的数据
        String del_A = "delete from \"stt\".t_weibo_relations where user_id = ?  by_type = 0";
        PhoenixUtilV2.delData(del_A,userid);

        //删除sel_list的数据
        String del_sel_list = "delete from \"stt\".t_weibo_relations where user_id = ? and rel_user_id = ? by_type = 1";
        for (String sel_id : sel_list) {
            PhoenixUtilV2.delData(del_sel_list,sel_id,userid);
        }


    }

    /**
     * 删除一条转发/评论关系
     */
    private static void delOne(WeiboRelations bean_by) {

        String del_one_sql = "delete from \"stt\".t_weibo_relations where user_id = ? and rel_type = ?  and rel_user_id = ? and by_type = ?";
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

    /**
     * 添加一条转发/评论关系
     * @param bean_by
     */
    private static void addOne(WeiboRelations bean_by) {
        //根据被转发关系构造一条主动转发关系
        WeiboRelations bean_self = new WeiboRelations(bean_by.getRel_user_id(), bean_by.getRel_type(), bean_by.getUser_id(),bean_by.getBy_type()==1?0:1);

        //分别插入2条数据

        String insert_rel_sql =  "UPSERT INTO \"stt\".T_WEIBO_RELATIONS (USER_ID,REL_TYPE,REL_USER_ID,BY_TYPE) " +
                " VALUES ( '" + bean_by.getUser_id()+"' ,'"+ bean_by.getRel_type()+"','"+ bean_by.getRel_user_id()+"',"+ bean_by.getBy_type()+" )";


        String insert_rel_sql2 =  "UPSERT INTO \"stt\".T_WEIBO_RELATIONS (USER_ID,REL_TYPE,REL_USER_ID,BY_TYPE) " +
                " VALUES ( '" + bean_self.getUser_id()+"' ,'"+bean_self.getRel_type()+"','"+bean_self.getRel_user_id()+"',"+bean_self.getBy_type()+" )";


        PhoenixUtilV2.insertData(insert_rel_sql);
        PhoenixUtilV2.insertData(insert_rel_sql2);
    }
}
