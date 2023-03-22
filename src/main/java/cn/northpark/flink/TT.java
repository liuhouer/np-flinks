package cn.northpark.flink;

import cn.hutool.core.codec.Base64;
import cn.hutool.http.HttpUtil;
import cn.northpark.flink.util.JsonUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

import java.util.Map;

/**
 * @author bruce
 * @date 2023年03月21日 16:51:37
 */
public class TT {
    public static void main(String[] args) {
        String decode = Base64.decodeStr("NzRhODVkNjFmYTkyYWZlMzViYzY2YmUyZjk1ZjBjMTY=");

//        String baseUrl = "https://restapi.amap.com/v3/geocode/geo?address="+"西山温泉"+"&key="+decode;
//        String res = HttpUtil.get(baseUrl);
//        Map<String, Object> json2map = JsonUtil.json2map(res);
//        System.err.println(json2map);
//        if(json2map.get("info").toString().equals("OK")){
//            Object geocodes = json2map.get("geocodes");
//            JSONArray jsonArray = JSON.parseArray(geocodes.toString());
//            Map<String, Object> geocodesMap = JsonUtil.json2map(jsonArray.get(0).toString());
//            String location = geocodesMap.get("location").toString();
//            String longitude = location.split(",")[0];
//            String latitude = location.split(",")[1];
//            System.err.println(longitude);
//            System.err.println(latitude);
//
//        }

        System.err.println(Double.valueOf("114.123456")==0d);
        System.err.println(Double.valueOf("0.000000")==0d);
    }
}
