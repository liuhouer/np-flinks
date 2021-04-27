/**
 * @author liuhouer
 * @date 2020年12月14日
 * 队列超时监控的实现 - 基于配置
 * <pre>
 *
 * 造数据：2分钟没支付就超时取消订单
 * {"userID": "user_5", "eventTime": "2020-12-15 14:29:13", "eventType": "create", "productID": "product_5", "productPrice": 20 ,"orderID":"1603349162707114"}
 *
 * {"userID": "user_5", "eventTime": "2020-12-15 14:13:18", "eventType": "pay", "productID": "product_5", "productPrice": 20 ,"orderID":"1603349162707114"}
 *
 * {"userID": "user_4", "eventTime": "2020-12-15 15:27:20", "eventType": "create", "productID": "product_5", "productPrice": 20 ,"orderID":"1603349424395132"}
 *
 * {"userID": "user_4", "eventTime": "2020-12-15 15:27:20", "eventType": "pay", "productID": "product_5", "productPrice": 20 ,"orderID":"1603349424395132"}
 *
 * 
 * property文件：
 * 
 * START_STATE = create
 * NEXT_STATE = pay
 * STATE_COL = eventType
 * MAX_DELAY_TIMEOUT  = 60000
 * CLASS_NAME = cn.northpark.XXX
 * PRIMARY_KEY = orderID
 * EVENT_TIME_COL = eventTime
 * QUEUE_NAME = flink4t
 * 
 * ====================================================================
 * CREATE TABLE "T_TIMEOUT_CFG" (
 * "QUEUE_NAME" VARCHAR(100 ) NOT NULL ,
 * "MAX_DELAY_TIMEOUT" VARCHAR(100 BYTE) NOT NULL ,
 * "START_STATE" VARCHAR(50 ) NOT NULL ,
 * "NEXT_STATE" VARCHAR(50 ) NOT NULL ,
 * "PRIMARY_KEY" VARCHAR(50 ) NOT NULL ,
 * "EVENT_TIME_COL" VARCHAR(100 ) NULL ,
 * "CLASS_NAME" VARCHAR(100 ) NULL ,
 * "STATE_COL" VARCHAR(100 ) NULL 
 * )
 *
 *
 *
 * ;
 * COMMENT ON COLUMN "T_TIMEOUT_CFG"."QUEUE_NAME" IS '队列名';
 * COMMENT ON COLUMN "T_TIMEOUT_CFG"."MAX_DELAY_TIMEOUT" IS '超时时间: ms';
 * COMMENT ON COLUMN "T_TIMEOUT_CFG"."START_STATE" IS '起始状态';
 * COMMENT ON COLUMN "T_TIMEOUT_CFG"."NEXT_STATE" IS '下个状态';
 * COMMENT ON COLUMN "T_TIMEOUT_CFG"."PRIMARY_KEY" IS '对应取数主键';
 * COMMENT ON COLUMN "T_TIMEOUT_CFG"."EVENT_TIME_COL" IS '事件时间字段';
 * COMMENT ON COLUMN "T_TIMEOUT_CFG"."CLASS_NAME" IS '对应实体';
 * COMMENT ON COLUMN "T_TIMEOUT_CFG"."STATE_COL" IS '状态取值字段';
 * ====================================================================
 *
 * </pre>
 */
public class TimeoutMonitor {



    private static OutputTag<TimeOutResult> timeoutTag = new OutputTag<TimeOutResult>("QueueTimeout"){};

    public static void main(String[] args) throws Exception {

        //1、解析参数
        String path = args[0];

        File file = new File(path);

        String jobName = file.getName().replace(".properties", "");
        System.err.println(jobName);

        InputStream in = new FileInputStream(file);

        RichParameterTool parameterTool = RichParameterTool.fromPropertiesFile(in);


        //以下字段从参数传递的配置文件读取
        //START_STATE,NEXT_STATE,STATE_COL,MAX_DELAY_TIMEOUT,CLASS_NAME,PRIMARY_KEY,EVENT_TIME_COL,QUEUE_NAME,EVENT_TIME_METHOD_NAME,KEY_BY_METHOD_NAME,STATE_COL_METHOD_NAME

        //开始状态
        String START_STATE = parameterTool.get("START_STATE");

        //下个状态
        String NEXT_STATE = parameterTool.get("NEXT_STATE");

        //状态取值
        String STATE_COL = parameterTool.get("STATE_COL");

        //超时时间 ms
        Long   MAX_DELAY_TIMEOUT  = parameterTool.getLong("MAX_DELAY_TIMEOUT");

        //对应实体
        String CLASS_NAME = parameterTool.get("CLASS_NAME");

        //对应主键
        String PRIMARY_KEY = parameterTool.get("PRIMARY_KEY");

        //事件时间字段
        String EVENT_TIME_COL = parameterTool.get("EVENT_TIME_COL");

        //队列名
        String QUEUE_NAME = parameterTool.get("QUEUE_NAME");

        //抽取事件时间方法名
        String EVENT_TIME_METHOD_NAME = "get"+ StringUtils.capitalize(EVENT_TIME_COL);

        //抽取key by方法名
        String KEY_BY_METHOD_NAME = "get"+ StringUtils.capitalize(PRIMARY_KEY);

        //抽取状态取值方法名
        String STATE_COL_METHOD_NAME = "get"+ StringUtils.capitalize(STATE_COL);


        //加载默认配置
        InputStream is = UDF_Test.class.getClassLoader().getResourceAsStream("config.properties");

        ParameterTool parameterToolBase = ParameterTool.fromPropertiesFile(is);
        String kafkaBootstrapServers = parameterToolBase.getRequired("kafkaBootstrapServers");
        String browseTopic = parameterTool.getRequired("QUEUE_NAME");
        String browseTopicGroupID = parameterToolBase.getRequired("browseTopicGroupID");


        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        streamEnv.setParallelism(1);

        //3、注册Kafka数据源
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers", kafkaBootstrapServers);
        browseProperties.put("group.id", browseTopicGroupID);
        DataStream<Object> browseStream = streamEnv
                .addSource(new FlinkKafkaConsumer<>(browseTopic, new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction(CLASS_NAME))
                .assignTimestampsAndWatermarks(new BrowseBoundedOutOfOrdernessTimestampExtractor(Time.seconds(5),CLASS_NAME, EVENT_TIME_METHOD_NAME));

        SingleOutputStreamOperator<String> timeOutedStream = browseStream.keyBy(t->{
            Method getPrimaryKeyM = t.getClass().getMethod(KEY_BY_METHOD_NAME);
            Object getPrimaryKey = getPrimaryKeyM.invoke(t);

            assert getPrimaryKey!=null;

            // 增加一个long类型的时间戳
            // 转换成毫秒时间戳
            return getPrimaryKey.toString();
        }).process(new OrderTimeoutFunction(START_STATE,NEXT_STATE,STATE_COL,MAX_DELAY_TIMEOUT,CLASS_NAME,PRIMARY_KEY,EVENT_TIME_COL,QUEUE_NAME,EVENT_TIME_METHOD_NAME,KEY_BY_METHOD_NAME,STATE_COL_METHOD_NAME));


        timeOutedStream.print();

        //测流输出超时未处理消息告警
        //队列超时监控的实现
        timeOutedStream.getSideOutput(timeoutTag).process(new ProcessFunction<TimeOutResult, Object>() {
            @Override
            public void processElement(TimeOutResult timeOutResult, Context context, Collector<Object> collector) throws Exception {
                //写入数据库
                TimeOutLogHelper.logMessage(timeOutResult,null);
            }
        }).print();


        streamEnv.execute(jobName);

    }


    /**
     * 解析Kafka数据
     */
    static class BrowseKafkaProcessFunction extends ProcessFunction<String, Object> {

        String CLASS_NAME;

        public BrowseKafkaProcessFunction(String class_name) {
            this.CLASS_NAME = class_name;
        }

        @Override
        public void processElement(String value, Context ctx, Collector<Object> out) throws Exception {
            try {

                Class<?> clz = Class.forName(CLASS_NAME);
                Object log = JSON.parseObject(value, clz);


                out.collect(log);
            } catch (Exception ex) {
                LoggerUtils.error("解析Kafka数据异常...", ex);
            }
        }
    }


    /**
     * 提取时间戳生成水印
     */
    static class BrowseBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Object> {
        String CLASS_NAME;
        String EVENT_TIME_METHOD_NAME;

        public BrowseBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness, String CLASS_NAME, String EVENT_TIME_METHOD_NAME) {
            super(maxOutOfOrderness);
            this.CLASS_NAME = CLASS_NAME;
            this.EVENT_TIME_METHOD_NAME = EVENT_TIME_METHOD_NAME;
        }

        BrowseBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Object element) {

            //===================================执行获取时间戳============================================
            Class<?> clz = null;
            Long eventTimeTimestamp = null;
            try {
                clz = Class.forName(CLASS_NAME);
                Method getEventTime = clz.getMethod(EVENT_TIME_METHOD_NAME);
                Object getEventTimeStr = getEventTime.invoke(element);

                assert getEventTimeStr!=null;

                // 增加一个long类型的时间戳
                // 转换成毫秒时间戳
                eventTimeTimestamp = TimeUtils.stringToMillis(getEventTimeStr.toString());

            } catch (Exception e) {
                e.printStackTrace();
            }


            //===================================执行获取时间戳============================================

            return eventTimeTimestamp;
        }
    }


    static class OrderTimeoutFunction extends ProcessFunction<Object, String> {


        /**
         * 这个状态是通过 ProcessFunction 维护
         */
        private ValueState<Boolean> isPayedState;
        private ValueState<Long> timerState;

        private String orderID;

        //开始状态
        String START_STATE  ;
        //下个状态
        String NEXT_STATE ;
        //状态取值
        String STATE_COL ;
        //超时时间 ms
        Long   MAX_DELAY_TIMEOUT  ;
        //对应实体
        String CLASS_NAME ;
        //对应主键
        String PRIMARY_KEY ;
        //事件时间字段
        String EVENT_TIME_COL ;
        //队列名
        String QUEUE_NAME ;
        //抽取事件时间方法名
        String EVENT_TIME_METHOD_NAME;
        //抽取key by方法名
        String KEY_BY_METHOD_NAME ;
        //抽取状态取值方法名
        String STATE_COL_METHOD_NAME ;

        public OrderTimeoutFunction(String START_STATE, String NEXT_STATE, String STATE_COL, Long MAX_DELAY_TIMEOUT, String CLASS_NAME, String PRIMARY_KEY, String EVENT_TIME_COL, String QUEUE_NAME, String EVENT_TIME_METHOD_NAME, String KEY_BY_METHOD_NAME, String STATE_COL_METHOD_NAME) {
            this.START_STATE = START_STATE;
            this.NEXT_STATE = NEXT_STATE;
            this.STATE_COL = STATE_COL;
            this.MAX_DELAY_TIMEOUT = MAX_DELAY_TIMEOUT;
            this.CLASS_NAME = CLASS_NAME;
            this.PRIMARY_KEY = PRIMARY_KEY;
            this.EVENT_TIME_COL = EVENT_TIME_COL;
            this.QUEUE_NAME = QUEUE_NAME;
            this.EVENT_TIME_METHOD_NAME = EVENT_TIME_METHOD_NAME;
            this.KEY_BY_METHOD_NAME = KEY_BY_METHOD_NAME;
            this.STATE_COL_METHOD_NAME = STATE_COL_METHOD_NAME;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<>("isPayedState", Boolean.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerState", Long.class));
        }

        @Override
        public void processElement(Object value, Context ctx, Collector<String> out) throws Exception {
            Class<?> clz = Class.forName(CLASS_NAME);
            Method getEventTime = clz.getMethod(EVENT_TIME_METHOD_NAME);
            Method getKeyBy = clz.getMethod(KEY_BY_METHOD_NAME);
            Method getStateCol = clz.getMethod(STATE_COL_METHOD_NAME);

            orderID = (String) getKeyBy.invoke(value);
            Boolean isPayed = isPayedState.value();
            Long timerTs = timerState.value();

            String stateVal = getStateCol.invoke(value).toString();
            Long eventTimeStamp = TimeUtils.stringToMillis(getEventTime.invoke(value).toString());
            if (stateVal.equals(START_STATE)) {  // 到来的事件是未处理事件
                // 乱序行为，先到pay再到create
                if (isPayed != null && isPayed) {
                    out.collect("payed successfully");
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                    isPayedState.clear();
                    timerState.clear();
                } else {
                    // 已创建订单未支付,设置定时器


                    Long ts = eventTimeStamp + MAX_DELAY_TIMEOUT ;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    timerState.update(ts);
                }
            } else if (stateVal.equals(NEXT_STATE)) {
                //假如有定时器，说明create过
                if (timerTs!=null && timerTs > 0) {
                    // timerTs 是 认为超时后的时间戳
                    if (timerTs > eventTimeStamp) {
                        out.collect("队列-主键号："+QUEUE_NAME+"-"+orderID+"--处理完成");
                    } else {
                        //定时器到时间不清除timerState，那么超时以后再来的队列会进入这个逻辑
                        ctx.output(timeoutTag, new TimeOutResult(QUEUE_NAME,orderID, "队列已超时未处理，超时后处理修正记录","1"));
                    }
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                    isPayedState.clear();
                    timerState.clear();
                } else {
                    // 先来pay,还未创建订单
                    isPayedState.update(true);
                    // 等待设置的超时时长的时间
                    Long ts = eventTimeStamp + MAX_DELAY_TIMEOUT;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    timerState.update(ts);
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            Boolean isPayed = isPayedState.value();
            if (isPayed != null && isPayed) {
                ctx.output(timeoutTag, new TimeOutResult(QUEUE_NAME,orderID, "已处理完成未创建队列" ,"0"));
            } else {
                // 典型场景，只create,没有pay
                ctx.output(timeoutTag, new TimeOutResult(QUEUE_NAME,orderID, "超时未处理队列" ,"0"));
            }

            isPayedState.clear();
            timerState.clear();

        }

    }

}
