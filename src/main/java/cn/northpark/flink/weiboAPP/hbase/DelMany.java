package cn.northpark.flink.weiboAPP.hbase;

import cn.northpark.flink.util.PhoenixUtilV2;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author bruce
 * @date 2022年06月26日 12:12:06
 */
public class DelMany {
    public static void main(String[] args) {
        //4.删除一批关系
        //把 A的所有【被转发/被评论关系】删除
        String userid = "75e1227c896a4eaa9fe782556af0fc76";
        delMany( userid);

    }

    private static void delMany(String userid) {
        //查询A的sub-list
        List<Map<String, String>> mapList = PhoenixUtilV2.queryList("select * from \"stt\".t_weibo_relations_v2 where rel_user_id = ? and by_type = 0",userid);
        List<String> sel_list = mapList.stream().filter(t -> Objects.nonNull(t.get("USER_ID"))).map(t -> t.get("USER_ID").toString()).collect(Collectors.toList());

        //删除A的数据
        String del_A = "delete from \"stt\".t_weibo_relations_v2 where user_id = ? and by_type = 1";
        PhoenixUtilV2.delData(del_A,userid);

        //删除sel_list的数据
        String del_sel_list = "delete from \"stt\".t_weibo_relations_v2 where user_id = ? and rel_user_id = ? and by_type = 0";
        for (String sel_id : sel_list) {
            PhoenixUtilV2.delData(del_sel_list,sel_id,userid);
        }


    }
}
