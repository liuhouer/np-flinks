package cn.northpark.flink.table_sql_api;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.InputStream;

/**
 * @author liuhouer
 * @Date 2020年09月29日 17:38:27
 * <p>
 * 实时数仓-数据变动关联更新维表join实现方式-步骤2:
 * 进行Flink 维表join处理
 * {"arrival_date":"20201027","arrival_time":"2020-10-27 13:43:54","des_col_name":"VC_SCODE","des_entity_code":"T_COMP_SECURITY","msg_format":"json","msg_reqno":"1603787032736128","msg_type":"single","source":"MD","src_col_name":"VC_SCODE","src_col_value":"204001337755","src_entity_code":"MD.T_BSC_SECURITY"}
 *
 *
 *  ./bin/flink run -m yarn-cluster   -ynm DimBondSQL   -c cn.northpark.specmsg.flinksql.DimBond3   -p 1 -yjm 1024m -ytm 6144m   /home/northpark/app/flink-1.11.1/NP-Flink-1.0.jar
 *
 *  ./bin/flink run   -ynm DimBondSQL   -c cn.northpark.specmsg.flinksql.DimBond3   -p 1 -yjm 1024m -ytm 6144m   /home/northpark/app/flink-1.11.1/NP-Flink-1.0.jar
 */
public class DimBond3 {

    public static final Boolean UPDATE_MODE = true;
    public static final Boolean INSERT_MODE = false;

    public static void main(String[] args) throws Exception {


        //1、解析参数
        InputStream is = DimBond3.class.getClassLoader().getResourceAsStream("config.properties");

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(is);
        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");
        String jdbcBondTopic = parameterTool.getRequired("JdbcBondTopic");
        String jdbcBondGroupID = parameterTool.getRequired("JdbcBondGroupID");


        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        EnvironmentSettings settings =  EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv,settings);


        //只有开启了checkpoint 才会有重启策略 默认是不重启
        // streamEnv.enableCheckpointing(30000, CheckpointingMode.AT_LEAST_ONCE);//每隔5s进行一次checkpoint

        //默认的重启策略是无限重启  Integer.MAX_VALUE 次
        streamEnv.setParallelism(1);

        //3、转化消息，发送到消息队列
        String readTopic = "DimBondTest";


        //4、 创建kafka数据源表，如下：不能是包含业务信息的message
        //经测试：kafka数据源映射类型不能为decimal和timestamp
        String T_BSC_SECURITY = "CREATE TABLE T_BSC_SECURITY (" +

                "  VC_SCODE                STRING  , " +
                "  VC_CODE                 STRING , " +
                "  VC_CODE_TYPE            STRING  , " +
                "  VC_ISIN                 STRING  , " +
                "  VC_TICKER               STRING  , " +
                "  VC_INNER_CODE_O32       STRING  , " +
                "  VC_INNER_CODE_GP3       STRING  , " +
                "  VC_RELATIVE_CODE        STRING  , " +
                "  VC_SNAME                STRING  , " +
                "  VC_NAME                 STRING  , " +
                "  VC_SPELL_ABBR           STRING  , " +
                "  VC_CURRENCY             STRING  , " +
                "  L_MARKET                INTEGER, " +
                "  EN_PAR_VALUE            DOUBLE  , " +
                "  VC_COMPANY_CODE         STRING  , " +
                "  VC_COMPANY_NAME         STRING  , " +
                "  L_OFFER_DATE            INTEGER, " +
                "  EN_ISSUE_PRICE          DOUBLE  , " +
                "  EN_ISSUE_SCALE          DOUBLE  , " +
                "  L_LIST_DATE             INTEGER, " +
                "  L_CANCEL_DATE           INTEGER, " +
                "  EN_MATURITY             DOUBLE  , " +
                "  C_VALID_FLAG            STRING  , " +
                "  VC_KIND                 STRING  , " +
                "  VC_KIND_TL4             STRING  , " +
                "  EN_COUPONRATE           DOUBLE  , " +
                "  L_BEGIN_DATE            INTEGER, " +
                "  L_CEASE_DATE            INTEGER, " +
                "  L_INTEREST_TYPE         INTEGER, " +
                "  L_INTERESTPAY_TYPE      INTEGER, " +
                "  L_INTECALC_RULE         INTEGER, " +
                "  C_IS_GUARANTY           STRING  , " +
                "  EN_GUARANTY_RATE        DOUBLE  , " +
                "  C_IS_CALLBOND           STRING  , " +
                "  L_CALL_DATE             INTEGER, " +
                "  C_IS_PUTBOND            STRING  , " +
                "  L_PUT_DATE              INTEGER, " +
                "  C_IF_GUARANTEE          STRING  , " +
                "  L_EXTCREDIT_ENHANCEMODE INTEGER, " +
                "  C_IS_SUSTAINBOND        STRING  , " +
                "  VC_BONDGROUP_CODE       STRING  , " +
                "  C_IS_PERPETUAL          STRING  , " +
                "  C_AS_BOND_OR_EQUITY     STRING  , " +
                "  VC_EQUITY_CODE          STRING  , " +
                "  TS_UPDATETIME           STRING  , " +
                "  VC_SOURCE               STRING  , " +
                "  VC_UPDATE_OPERATER      STRING  , " +
                "  D_UPDATETIME            STRING, " +
                "  L_END_DATE              INTEGER, " +
                "  VC_KIND_IMS_ATYPE       STRING  , " +
                "  L_DELETE                INTEGER     , " +
                "  D_TAR_UPDATETIME        STRING   , " +
                "  VC_BCODE                STRING ," +
                "   PROCTIME AS PROCTIME() " +
                " ) WITH (" +

                "   'connector.type' = 'kafka'," +
                "   'connector.topic' = '" + readTopic + "'," +
                "   'connector.version' = 'universal'," +
                "   'connector.startup-mode' = 'latest-offset'," +
                "   'connector.properties.group.id' = 'bruce'," +
                "   'connector.properties.zookeeper.connect' = 'node1:2181,node2:2181,node3:2181'," +
                "   'connector.properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
//                "   'update-mode' = 'append' ," +
                "   'format.type' = 'json'" +
                " ) ";


        tEnv.executeSql(T_BSC_SECURITY);


        //5> 结果表和关联维表的注册
        // todo 从配置中根据from to 找到涉及到的维表，从模板读取维表写好的注册语句 failed

        //结果表注册，用于写入

        String T_COMP_SECURITY = " create table T_COMP_SECURITY " +
                " ( " +
                "  VC_SCODE                STRING , " +
                "  VC_CODE                 STRING , " +
                "  L_MARKET                DECIMAL, " +
                "  VC_INNER_CODE_O32       STRING , " +
                "  VC_INNER_CODE_GP3       STRING , " +
                "  VC_RELATIVE_CODE        STRING , " +
                "  VC_SNAME                STRING , " +
                "  VC_NAME                 STRING , " +
                "  VC_SPELL_ABBR           STRING , " +
                "  VC_CURRENCY             STRING , " +
                "  VC_COMPANY_CODE         STRING , " +
                "  VC_COMPANY_NAME         STRING , " +
                "  VC_COMPANY_TYPE         DECIMAL, " +
                "  L_AREA_CODE             DECIMAL, " +
                "  L_AREA_NAME             STRING , " +
                "  VC_COMPANY_PROPERTY     STRING , " +
                "  L_OFFER_DATE            DECIMAL, " +
                "  EN_ISSUE_PRICE          DECIMAL , " +
                "  L_LIST_DATE             DECIMAL, " +
                "  L_CANCEL_DATE           DECIMAL, " +
                "  EN_MATURITY             DECIMAL , " +
                "  C_VALID_FLAG            CHAR , " +
                "  VC_KIND_TL4_LV1         STRING , " +
                "  VC_KIND_TL4_LV2         STRING , " +
                "  VC_KIND_TL4_LV3         STRING , " +
                "  VC_KIND_TL4_LV4         STRING , " +
                "  VC_KIND_TL4_LV5         STRING , " +
                "  VC_KIND_TL4_LV6         STRING , " +
                "  VC_KIND_TL4             STRING , " +
                "  VC_KIND_IMS_LV1         STRING , " +
                "  VC_KIND_IMS_LV2         STRING , " +
                "  VC_KIND_IMS_LV3         STRING , " +
                "  VC_KIND_IMS_LV4         STRING , " +
                "  VC_KIND_IMS_LV5         STRING , " +
                "  VC_KIND_IMS_ATYPE       STRING , " +
                "  EN_PAR_VALUE            DECIMAL , " +
                "  EN_COUPONRATE           DECIMAL , " +
                "  L_BEGIN_DATE            DECIMAL, " +
                "  L_CEASE_DATE            DECIMAL, " +
                "  L_END_DATE              DECIMAL, " +
                "  L_STRIKE_DATE           DECIMAL, " +
                "  L_INTEREST_TYPE         DECIMAL, " +
                "  STOCK_INDUSTRY1         STRING , " +
                "  STOCK_INDUSTRY1_NAME    STRING  , " +
                "  STOCK_INDUSTRY_MIX      STRING , " +
                "  STOCK_INDUSTRY_MIX_NAME STRING , " +
                "  INDUSTRY_GICS           STRING , " +
                "  INDUSTRY_GICS_NAME      STRING  , " +
                "  INDUSTRY_SW1            STRING , " +
                "  INDUSTRY_SW1_NAME       STRING  , " +
                "  INDUSTRY_SW2            STRING , " +
                "  INDUSTRY_SW2_NAME       STRING  , " +
                "  INDUSTRY_SW3            STRING , " +
                "  INDUSTRY_SW3_NAME       STRING  , " +
                "  INDUSTRY_TK1            STRING , " +
                "  INDUSTRY_TK1_NAME       STRING , " +
                "  C_IS_CALLBOND           CHAR , " +
                "  L_CALL_DATE             DECIMAL, " +
                "  C_IS_PUTBOND            CHAR , " +
                "  L_PUT_DATE              DECIMAL, " +
                "  C_IF_GUARANTEE          CHAR , " +
                "  C_IS_PERPETUAL          CHAR , " +
                "  C_AS_BOND_OR_EQUITY     CHAR , " +
                "  TS_UPDATETIME           TIMESTAMP, " +
                "  D_UPDATETIME            TIMESTAMP ," +
                "  PRIMARY KEY(VC_SCODE,VC_CODE)  NOT ENFORCED " + //设置入库主键以后，自动适配insert和update 测试通过2020-11-16 14:47:56

                " ) WITH (" +

                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:oracle:thin:@oradb:1521:imsuat'," +
                "   'table-name' = 'T_COMP_SECURITY'," +
                "   'username' = 'risk'," +
                "   'password' = 'risk'" +

                " )";


        tEnv.executeSql(T_COMP_SECURITY);

        //维表b
        String T_BSC_INSTITUTIONPROFILE = "CREATE TABLE T_BSC_INSTITUTIONPROFILE (" +

                " VC_COMPANY_CODE           STRING    ," +
                "  VC_PARENT_COMPANY         STRING   ," +
                "  VC_COMPANY_SNAME          STRING   ," +
                "  VC_COMPANY_NAME           STRING   ," +
                "  L_ESTABLISH_DATE           DECIMAL," +
                "  L_COMPANY_TYPE             DECIMAL  ," +
                "  VC_COUNTRY_NO             STRING ," +
                "  L_AREA_NO                  DECIMAL," +
                "  D_UPDATETIME              TIMESTAMP  ," +
                "  L_FIRST_DATE               DECIMAL," +
                "  L_UPDATE_DATE              DECIMAL," +
                "  VC_COMPANY_EN_NAME        STRING ," +
                "  VC_COMPANY_EN_SNAME       STRING ," +
                "  C_IS_BRANCH_COMPANY       STRING," +
                "  VC_LEGAL_REPRESENTATIVE   STRING  ," +
                "  VC_CHAIRMAN               STRING  ," +
                "  VC_GENERAL_MANAGER        STRING  ," +
                "  VC_BOARD_SECRETARY        STRING  ," +
                "  VC_BS_TEL                 STRING  ," +
                "  VC_BS_FAX                 STRING  ," +
                "  VC_BS_EMAIL               STRING  ," +
                "  VC_BS_AUTH_REPRESENT      STRING  ," +
                "  VC_SECU_AFF_REPRESENT     STRING  ," +
                "  VC_SAR_TEL                STRING  ," +
                "  VC_SAR_FAX                STRING  ," +
                "  VC_SAR_EMAIL              STRING  ," +
                "  VC_INST_PROPERTY          STRING ," +
                "  L_CURRENCY                 DECIMAL," +
                "  EN_REG_CAPITAL            DECIMAL," +
                "  VC_REG_ADDRESS            STRING ," +
                "  VC_REG_POSTCODE           STRING ," +
                "  VC_ADDRESS                STRING ," +
                "  VC_POSTCODE               STRING ," +
                "  VC_COMP_TEL               STRING ," +
                "  VC_FAX                    STRING ," +
                "  VC_EMAIL                  STRING ," +
                "  VC_WEB                    STRING ," +
                "  VC_SERV_TEL               STRING ," +
                "  VC_COMP_EVOLUT            STRING ," +
                "  VC_BUS_SCOPE              STRING ," +
                "  VC_MAIN_BUS               STRING ," +
                "  L_REG_DATE                 DECIMAL," +
                "  VC_LICENSE_NO             STRING," +
                "  VC_TAX_REG_NO             STRING," +
                "  VC_LOCAL_TAX_REG_NO       STRING," +
                "  VC_DISCLOSEINFO_WEB       STRING," +
                "  VC_DISCLOSEINFO_NEWSPAPER STRING," +
                "  L_STATUS                   DECIMAL," +
                "  VC_INST_LVL                DECIMAL," +
                "  C_IS_LISTED               STRING," +
                "  EN_AUTH_CAP_STOCK         DECIMAL," +
                "  VC_INST_TYPE              STRING," +
                "  VC_SERV_FAX               STRING," +
                "  L_EMPLOYEES_NO             DECIMAL," +
                "  L_EMPLOYEES_NO_DATE       STRING," +
                "  L_BUS_SCALE                DECIMAL," +
                "  VC_REGION2_NAME           STRING," +
                "  VC_MD5                    STRING," +
                "  VC_SOURCE                 STRING," +
                "  VC_UPDATE_OPERATER        STRING," +
                "  L_DELETE                   DECIMAL ," +
                "  D_TAR_UPDATETIME          TIMESTAMP ," +
                "  L_INS_FLAG                 DECIMAL," +
                "  VC_SRC_COMPANY_TYPE       STRING " +
                " ) WITH (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:oracle:thin:@oradb:1521:imsuat'," +
                "   'table-name' = 'MD.T_BSC_INSTITUTIONPROFILE'," +
                "   'username' = 'risk'," +
                "   'password' = 'risk'" +
                " )";

        tEnv.executeSql(T_BSC_INSTITUTIONPROFILE);


        //维表c
        String T_BSC_INSTITUTIONINDUSTRY = "CREATE TABLE T_BSC_INSTITUTIONINDUSTRY (" +

                "VC_COMPANY_CODE     STRING   , " +
                "  L_BEGIN_DATE       DECIMAL  , " +
                "  L_END_DATE         DECIMAL  , " +
                "  L_TYPE_NO          DECIMAL  , " +
                "  VC_ENGLISH_NAME     STRING , " +
                "  VC_ONE_CODE         STRING , " +
                "  VC_ONE_NAME         STRING , " +
                "  VC_TWO_CODE         STRING , " +
                "  VC_TWO_NAME         STRING , " +
                "  VC_THREE_CODE       STRING , " +
                "  VC_THREE_NAME       STRING , " +
                "  VC_FOUR_CODE        STRING , " +
                "  VC_FOUR_NAME        STRING , " +
                "  C_NEW_EQUITY       STRING, " +
                "  C_FIRST_INCOME     STRING, " +
                "  D_UPDATETIME       TIMESTAMP  , " +
                "  VC_MD5             STRING, " +
                "  VC_SOURCE           STRING , " +
                "  VC_UPDATE_OPERATER  STRING , " +
                "  L_DELETE           DECIMAL  , " +
                "  D_TAR_UPDATETIME   TIMESTAMP  " +

                " ) WITH (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:oracle:thin:@oradb:1521:imsuat'," +
                "   'table-name' = 'MD.T_BSC_INSTITUTIONINDUSTRY'," +
                "   'username' = 'risk'," +
                "   'password' = 'risk'" +
                " )";

        tEnv.executeSql(T_BSC_INSTITUTIONINDUSTRY);


        //维表d
        String T_COMP_DIM_VAL_DIC = "CREATE TABLE T_COMP_DIM_VAL_DIC (" +
                "   L_SERIAL_ID     DECIMAL   ,     " +
                "   VC_DIM_CODE     STRING   ,     " +
                "   VC_ITEM_VALUE   STRING   ,     " +
                "   VC_ITEM_NAME    STRING,     " +
                "   VC_VAL_EXPR     STRING ,     " +
                "   L_DISPLAY_ORDER DECIMAL,     " +
                "   C_STATUS        CHAR ,     " +
                "   VC_DIM_DISPLAY  STRING   ,     " +
                "   VC_VALUE_DESC   STRING,     " +
                "   T_MDF_TIME      TIMESTAMP ,     " +
                "   VC_USER_ID      STRING      " +
                "  ) WITH (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:oracle:thin:@oradb:1521:imsuat'," +
                "   'table-name' = 'RISK.T_COMP_DIM_VAL_DIC'," +
                "   'username' = 'risk'," +
                "   'password' = 'risk'" +
                " )";

        tEnv.executeSql(T_COMP_DIM_VAL_DIC);


        // todo 不能用的函数 -需要替换成flinksql中的内置函数 TO_CHAR |  NVL | LEAST |
        //CAST(B.L_AREA_NO AS String)
        //测试join数据打印
//        String joinSql = "  SELECT A.VC_SCODE, A.VC_CODE, CAST(A.L_MARKET AS DECIMAL), A.VC_INNER_CODE_O32, A.VC_INNER_CODE_GP3,  " +
//                "   A.VC_RELATIVE_CODE, A.VC_SNAME, A.VC_NAME, A.VC_SPELL_ABBR, A.VC_CURRENCY, A.VC_COMPANY_CODE,   " +
//                "  B.VC_COMPANY_NAME, B.L_COMPANY_TYPE AS VC_COMPANY_TYPE, B.L_AREA_NO AS L_AREA_CODE, D .VC_ITEM_NAME AS L_AREA_NAME,   " +
//                "  B.VC_INST_PROPERTY AS VC_COMPANY_PROPERTY, A.L_OFFER_DATE, A.EN_ISSUE_PRICE, A.L_LIST_DATE, A.L_CANCEL_DATE,   " +
//                "  A.EN_MATURITY, A.C_VALID_FLAG, SUBSTR (A.VC_KIND_TL4, 1, 1) AS VC_KIND_TL4_LV1, SUBSTR (A.VC_KIND_TL4, 1, 2) AS VC_KIND_TL4_LV2,   " +
//                "  SUBSTR (A.VC_KIND_TL4, 1, 3) AS VC_KIND_TL4_LV3, SUBSTR (A.VC_KIND_TL4, 1, 4) AS VC_KIND_TL4_LV4, SUBSTR (A.VC_KIND_TL4, 1, 5) AS VC_KIND_TL4_LV5,   " +
//                "  SUBSTR (A.VC_KIND_TL4, 1, 6) AS VC_KIND_TL4_LV6, A.VC_KIND_TL4, SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 2) AS VC_KIND_IMS_LV1, SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 3)   " +
//                "  AS VC_KIND_IMS_LV2, SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 4) AS VC_KIND_IMS_LV3, SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 5) AS VC_KIND_IMS_LV4,   " +
//                "  SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 6) AS VC_KIND_IMS_LV5, A.VC_KIND_IMS_ATYPE, A.EN_PAR_VALUE, A.EN_COUPONRATE, A.L_BEGIN_DATE, A.L_CEASE_DATE,   " +
//                "  A.L_END_DATE," +
//                // "  LEAST ( NVL (A.L_CALL_DATE, 21991231), NVL (A.L_PUT_DATE, 21991231), A.L_END_DATE ) AS L_STRIKE_DATE, " +
//                "  A.L_INTEREST_TYPE,  " +
//                "   C.VC_ONE_CODE AS STOCK_INDUSTRY1, C.VC_ONE_NAME AS STOCK_INDUSTRY1_NAME, H .VC_ONE_CODE AS STOCK_INDUSTRY_MIX, H .VC_ONE_NAME AS STOCK_INDUSTRY_MIX_NAME,   " +
//                "  E .VC_ONE_CODE AS INDUSTRY_GICS, E .VC_ONE_NAME AS INDUSTRY_GICS_NAME, F.VC_ONE_CODE AS INDUSTRY_SW1, F.VC_ONE_NAME AS INDUSTRY_SW1_NAME,   " +
//                "  F.VC_TWO_CODE AS INDUSTRY_SW2, F.VC_TWO_NAME AS INDUSTRY_SW2_NAME, F.VC_THREE_CODE AS INDUSTRY_SW3, F.VC_THREE_NAME AS INDUSTRY_SW3_NAME,   " +
//                "  G .VC_ONE_CODE AS INDUSTRY_TK1, G .VC_ONE_NAME AS INDUSTRY_TK1_NAME, A.C_IS_CALLBOND, A.L_CALL_DATE, A.C_IS_PUTBOND, A.L_PUT_DATE,   " +
//                "  A.C_IF_GUARANTEE, A.C_IS_PERPETUAL, A.C_AS_BOND_OR_EQUITY  ," +
//                //"  TO_CHAR ( A.D_TAR_UPDATETIME, 'YYYY-MM-DD HH24:MI:SS' ) AS TS_UPDATETIME,   " +
//                //"  TO_CHAR ( SYSDATE, 'YYYY-MM-DD HH24:MI:SS' ) AS D_UPDATETIME " +
////                "  TO_TIMESTAMP(A.D_TAR_UPDATETIME, 'YYYY-MM-DD HH24:MI:SS' ) AS TS_UPDATETIME   " +
//                "  CURRENT_TIMESTAMP  " +
//                "  FROM T_BSC_SECURITY A LEFT JOIN T_BSC_INSTITUTIONPROFILE B   " +
//                "  ON A.VC_COMPANY_CODE = B.VC_COMPANY_CODE " +
//                "  AND B.L_DELETE = 0 LEFT JOIN T_COMP_DIM_VAL_DIC D " +
//                "  ON CAST(B.L_AREA_NO AS String)  = D .VC_ITEM_VALUE   " +
//                "  AND D .VC_DIM_CODE = 'ISSUER_AREA' AND D .C_STATUS = '1' LEFT JOIN T_BSC_INSTITUTIONINDUSTRY C " +
//                "  ON A.VC_COMPANY_CODE = C.VC_COMPANY_CODE   " +
//                "  AND C.L_END_DATE = 20990101 AND C.L_TYPE_NO = 117 AND C.L_DELETE = 0 LEFT JOIN T_BSC_INSTITUTIONINDUSTRY E " +
//                "  ON A.VC_COMPANY_CODE = E .VC_COMPANY_CODE   " +
//                "  AND E .L_END_DATE = 20990101 AND E .L_TYPE_NO = 107 AND E .L_DELETE = 0 LEFT JOIN T_BSC_INSTITUTIONINDUSTRY F " +
//                "  ON A.VC_COMPANY_CODE = F.VC_COMPANY_CODE   " +
//                "  AND F.L_END_DATE = 20990101 AND F.L_TYPE_NO = 122 AND F.L_DELETE = 0 LEFT JOIN T_BSC_INSTITUTIONINDUSTRY G " +
//                "  ON A.VC_COMPANY_CODE = G .VC_COMPANY_CODE   " +
//                "  AND G .L_END_DATE = 20990101 AND G .L_TYPE_NO = 900 AND G .L_DELETE = 0 LEFT JOIN ( SELECT VC_ONE_CODE, VC_ONE_NAME, VC_COMPANY_CODE   " +
//                "  FROM T_BSC_INSTITUTIONINDUSTRY WHERE L_END_DATE = 20990101 AND L_TYPE_NO = 117 AND VC_ONE_CODE NOT IN ('C') AND L_DELETE = 0 UNION SELECT VC_TWO_CODE,   " +
//                "  VC_TWO_NAME, VC_COMPANY_CODE FROM T_BSC_INSTITUTIONINDUSTRY WHERE L_END_DATE = 20990101 AND L_TYPE_NO = 117 AND VC_ONE_CODE = 'C' AND L_DELETE = 0 ) H   " +
//                "  ON A.VC_COMPANY_CODE = H .VC_COMPANY_CODE WHERE A.L_DELETE = 0 ";
//
//        Table table = tEnv.sqlQuery(joinSql);
//        tEnv.toRetractStream(table, Row.class).print();

        //测试flink不兼容的oracle函数和类型
        //TODO ERROR! --- DECIMAL(10, 0) and VARCHAR(2147483647) does not have common type now - join类型不一致[已找到原因]

        //可以在前一步执行删除，这一步直接insert
        // DATE_FORMAT(A.D_TAR_UPDATETIME, 'YYYY-MM-DD HH24:MI:SS' ) AS TS_UPDATETIME,
        // FROM_UNIXTIME(CAST (NOW() AS INTEGER)) AS D_UPDATETIME " +
        //L_STRIKE_DATE
        //TS_UPDATETIME
        //D_UPDATETIME

        //INSERT todo 尝试转化为Retract模式【2条消息:delete然后insert】【upsert为1条消息】
        String joinAndInsertSql = " INSERT INTO T_COMP_SECURITY  SELECT A.VC_SCODE, A.VC_CODE, CAST(A.L_MARKET AS DECIMAL) AS L_MARKET, A.VC_INNER_CODE_O32, A.VC_INNER_CODE_GP3,  " +
                "   A.VC_RELATIVE_CODE, A.VC_SNAME, A.VC_NAME, A.VC_SPELL_ABBR, A.VC_CURRENCY, A.VC_COMPANY_CODE,   " +
                "  B.VC_COMPANY_NAME, B.L_COMPANY_TYPE AS VC_COMPANY_TYPE, B.L_AREA_NO AS L_AREA_CODE, D .VC_ITEM_NAME AS L_AREA_NAME,   " +
                "  B.VC_INST_PROPERTY AS VC_COMPANY_PROPERTY, CAST(A.L_OFFER_DATE AS DECIMAL) AS L_OFFER_DATE, CAST(A.EN_ISSUE_PRICE AS DECIMAL) AS EN_ISSUE_PRICE, CAST(A.L_LIST_DATE AS DECIMAL) AS L_LIST_DATE, CAST(A.L_CANCEL_DATE AS DECIMAL) AS L_CANCEL_DATE,   " +
                "  CAST(A.EN_MATURITY AS DECIMAL) AS EN_MATURITY , CAST(A.C_VALID_FLAG AS Char) AS C_VALID_FLAG, SUBSTR (A.VC_KIND_TL4, 1, 1) AS VC_KIND_TL4_LV1, SUBSTR (A.VC_KIND_TL4, 1, 2) AS VC_KIND_TL4_LV2,   " +
                "  SUBSTR (A.VC_KIND_TL4, 1, 3) AS VC_KIND_TL4_LV3, SUBSTR (A.VC_KIND_TL4, 1, 4) AS VC_KIND_TL4_LV4, SUBSTR (A.VC_KIND_TL4, 1, 5) AS VC_KIND_TL4_LV5,   " +
                "  SUBSTR (A.VC_KIND_TL4, 1, 6) AS VC_KIND_TL4_LV6, A.VC_KIND_TL4, SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 2) AS VC_KIND_IMS_LV1, SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 3)   " +
                "  AS VC_KIND_IMS_LV2, SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 4) AS VC_KIND_IMS_LV3, SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 5) AS VC_KIND_IMS_LV4,   " +
                "  SUBSTR (A.VC_KIND_IMS_ATYPE, 1, 6) AS VC_KIND_IMS_LV5, A.VC_KIND_IMS_ATYPE, " +
                "  CAST(A.EN_PAR_VALUE AS DECIMAL) AS EN_PAR_VALUE  , CAST(A.EN_COUPONRATE AS DECIMAL) AS EN_COUPONRATE ,CAST(A.L_BEGIN_DATE AS DECIMAL) AS L_BEGIN_DATE  " +
                " , CAST(A.L_CEASE_DATE AS DECIMAL) AS L_CEASE_DATE ,   " +
                "  CAST(A.L_END_DATE AS DECIMAL) AS L_END_DATE ," +
                // "  LEAST ( COALESCE (A.L_CALL_DATE, 21991231), COALESCE (A.L_PUT_DATE, 21991231), A.L_END_DATE ) AS L_STRIKE_DATE, " +
                "  CAST('21991231' AS DECIMAL) AS L_STRIKE_DATE ,  " +
                "  CAST(A.L_INTEREST_TYPE AS DECIMAL) AS L_INTEREST_TYPE ,  " +
                "   C.VC_ONE_CODE AS STOCK_INDUSTRY1, C.VC_ONE_NAME AS STOCK_INDUSTRY1_NAME, H .VC_ONE_CODE AS STOCK_INDUSTRY_MIX, H .VC_ONE_NAME AS STOCK_INDUSTRY_MIX_NAME,   " +
                "  E .VC_ONE_CODE AS INDUSTRY_GICS, E .VC_ONE_NAME AS INDUSTRY_GICS_NAME, F.VC_ONE_CODE AS INDUSTRY_SW1, F.VC_ONE_NAME AS INDUSTRY_SW1_NAME,   " +
                "  F.VC_TWO_CODE AS INDUSTRY_SW2, F.VC_TWO_NAME AS INDUSTRY_SW2_NAME, F.VC_THREE_CODE AS INDUSTRY_SW3, F.VC_THREE_NAME AS INDUSTRY_SW3_NAME,   " +
                "  G .VC_ONE_CODE AS INDUSTRY_TK1, G .VC_ONE_NAME AS INDUSTRY_TK1_NAME, CAST(A.C_IS_CALLBOND AS Char) AS C_IS_CALLBOND , " +
                "  CAST(A.L_CALL_DATE AS DECIMAL) AS L_CALL_DATE , CAST(A.C_IS_PUTBOND AS Char) AS C_IS_PUTBOND , CAST(A.L_PUT_DATE AS DECIMAL) AS L_PUT_DATE,   " +
                "  CAST(A.C_IF_GUARANTEE AS Char) AS C_IF_GUARANTEE ,  CAST(A.C_IS_PERPETUAL AS Char) AS C_IS_PERPETUAL , CAST(A.C_AS_BOND_OR_EQUITY AS Char) AS C_AS_BOND_OR_EQUITY  , " +
                "  TO_TIMESTAMP(A.D_TAR_UPDATETIME, 'YYYY-MM-DD HH:MM:SS' ) AS TS_UPDATETIME,   " +
                "  LOCALTIMESTAMP AS D_UPDATETIME " +
                "  FROM T_BSC_SECURITY A LEFT JOIN T_BSC_INSTITUTIONPROFILE B   " +
                "  ON A.VC_COMPANY_CODE = B.VC_COMPANY_CODE " +
                "  AND B.L_DELETE = 0 LEFT JOIN T_COMP_DIM_VAL_DIC D " +
                "  ON CAST(B.L_AREA_NO AS String)  = D .VC_ITEM_VALUE   " +
                "  AND D .VC_DIM_CODE = 'ISSUER_AREA' AND D .C_STATUS = '1' LEFT JOIN T_BSC_INSTITUTIONINDUSTRY C " +
                "  ON A.VC_COMPANY_CODE = C.VC_COMPANY_CODE   " +
                "  AND C.L_END_DATE = 20990101 AND C.L_TYPE_NO = 117 AND C.L_DELETE = 0 LEFT JOIN T_BSC_INSTITUTIONINDUSTRY E " +
                "  ON A.VC_COMPANY_CODE = E .VC_COMPANY_CODE   " +
                "  AND E .L_END_DATE = 20990101 AND E .L_TYPE_NO = 107 AND E .L_DELETE = 0 LEFT JOIN T_BSC_INSTITUTIONINDUSTRY F " +
                "  ON A.VC_COMPANY_CODE = F.VC_COMPANY_CODE   " +
                "  AND F.L_END_DATE = 20990101 AND F.L_TYPE_NO = 122 AND F.L_DELETE = 0 LEFT JOIN T_BSC_INSTITUTIONINDUSTRY G " +
                "  ON A.VC_COMPANY_CODE = G .VC_COMPANY_CODE   " +
                "  AND G .L_END_DATE = 20990101 AND G .L_TYPE_NO = 900 AND G .L_DELETE = 0 LEFT JOIN ( SELECT VC_ONE_CODE, VC_ONE_NAME, VC_COMPANY_CODE   " +
                "  FROM T_BSC_INSTITUTIONINDUSTRY WHERE L_END_DATE = 20990101 AND L_TYPE_NO = 117 AND VC_ONE_CODE NOT IN ('C') AND L_DELETE = 0 UNION SELECT VC_TWO_CODE,   " +
                "  VC_TWO_NAME, VC_COMPANY_CODE FROM T_BSC_INSTITUTIONINDUSTRY WHERE L_END_DATE = 20990101 AND L_TYPE_NO = 117 AND VC_ONE_CODE = 'C' AND L_DELETE = 0 ) H   " +
                "  ON A.VC_COMPANY_CODE = H .VC_COMPANY_CODE WHERE A.L_DELETE = 0 ";


        tEnv.executeSql(joinAndInsertSql);


//        streamEnv.execute("DimBondPhase2");


    }

}
