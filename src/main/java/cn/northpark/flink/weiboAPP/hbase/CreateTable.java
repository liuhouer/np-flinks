package cn.northpark.flink.weiboAPP.hbase;

import cn.northpark.flink.util.PhoenixUtilV2;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author bruce
 * @date 2022年06月26日 10:03:33
 */
public class CreateTable {

    public static void main(String[] args) {

        //1.建表
        String t_weibo_relations_sql = "CREATE TABLE \"stt\".T_WEIBO_RELATIONS_V2 (\n" +
                " ID VARCHAR NOT NULL,\n" +
                " USER_ID VARCHAR ,\n" +
                " REL_TYPE VARCHAR,\n" +
                " REL_USER_ID VARCHAR,\n" +
                " BY_TYPE INTEGER\n" +
                " CONSTRAINT PK PRIMARY KEY (ID)\n" +
                ")";
        PhoenixUtilV2.createTable(t_weibo_relations_sql);



    }

}
