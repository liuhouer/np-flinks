package cn.northpark.flink.project.syncIO.function;


import cn.northpark.flink.project.ActivityBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * 经纬度返回实体类
 */
public class AsyncRestfulToActivityBeanFunciton extends RichAsyncFunction<String, ActivityBean> {

    public static final String GAODE_API_KEY = "4924f7ef5c86a278f5500851541cdcff";

    //不序列化 httpclient --不参与checkpoint
    private transient CloseableHttpAsyncClient httpclient = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpclient = HttpAsyncClients.createDefault();

        //初始化异步的HttpClient
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();
        httpclient = HttpAsyncClients.custom()
                .setMaxConnTotal(20)
                .setDefaultRequestConfig(requestConfig)
                .build();
        httpclient.start();
    }


//        String[] fields = line.split( ",");
//        //u001,A1,2019-09-02 10:10:11,1, 115.908923,39.267291
//
//        String uid = fields[0] ;
//        String aid = fields[1] ;
//        String time = fields[2] ;
//        int eventType = Integer.parseInt(fields[3]) ;
//        String longitude = fields[4] ;
//        String latitude =  fields[5] ;
//        String province = null;
//        String url = "https://restapi.amap.com/v3/geocode/regeo?key="+GAODE_API_KEY+"&location="+longitude+","+latitude;
//
//
//
//        HttpGet httpGet = new HttpGet(url);
//        CloseableHttpResponse response = httpclient.execute(httpGet);
//        try {
//            int status = response . getStatusLine() .getStatusCode() ;
//            if (status == 200) {
//                //获取请求的json字符串
//                String result = EntityUtils.toString( response . getEntity());
//                System.out.println(result);
//                //转成json对象
//                JSONObject jsonObj = JSON.parseObject(result);
//                //获取位置信息
//                JSONObject regeocode = jsonObj.getJSONObject("regeocode");
//                if (regeocode != null && ! regeocode.isEmpty()) {
//                    JSONObject address = regeocode.getJSONObject ("addressComponent");
//                //获取省市区
//                    province = address.getString("province");
//                    //String city = address .getString("city");
//                    //String businessAreas = address. getString("businessAreas");
//                }
//            }
//        } finally {
//            response.close();
//        }
//
//        return ActivityBean.of(uid,aid,null,time,eventType,province,longitude,latitude);

    @Override
    public void asyncInvoke(String line, ResultFuture<ActivityBean> resultFuture) throws Exception {
        String[] fields = line.split( ",");
        //u001,A1,2019-09-02 10:10:11,1,115.908923,39.267291
        String uid = fields[0] ;
        String aid = fields[1] ;
        String time = fields[2] ;
        int eventType = Integer.parseInt(fields[3]) ;
        String longitude = fields[4] ;
        String latitude =  fields[5] ;
        String province = null;
        String url = "https://restapi.amap.com/v3/geocode/regeo?key="+GAODE_API_KEY+"&location="+longitude+","+latitude;
        HttpGet httpGet = new HttpGet(url);
        Future<HttpResponse> responseFuture = httpclient.execute(httpGet, null);
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                String province = null;
                try {
                    HttpResponse httpResponse = responseFuture.get();
                    if(httpResponse.getStatusLine().getStatusCode() == 200){

                        //获取请求的json字符串
                        String result = EntityUtils.toString(httpResponse.getEntity());
                        System.out.println(result);
                        //转成json对象
                        JSONObject jsonObj = JSON.parseObject(result);
                        //获取位置信息
                        JSONObject regeocode = jsonObj.getJSONObject("regeocode");
                        if (regeocode != null && !regeocode.isEmpty()) {
                            JSONObject address = regeocode.getJSONObject("addressComponent");
                            //获取省市区
                            province = address.getString("province");
                            //String city = address .getString("city");
                            //String businessAreas = address. getString("businessAreas");
                        }

                    }
                    return province;
                }catch (Exception e){
                    return null;
                }

            }
        }).thenAccept((String rs) ->{
            resultFuture.complete(Collections.singleton(ActivityBean.of(uid,aid,null,time,eventType, rs)));
        });


    }

    @Override
    public void close() throws Exception {
        super.close();
        httpclient.close();
    }


}
