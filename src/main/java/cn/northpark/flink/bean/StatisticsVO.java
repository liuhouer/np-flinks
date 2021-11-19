package cn.northpark.flink.bean;

import java.util.Map;

/**
 * @author bruce
 * @date 2021年11月17日 16:24:01
 */
public class StatisticsVO {
     public String url ;
     public String method ;
     public String ip ;
     public String class_method ;
     public String args ;

     public UserVO userVO;

     public Map<String, String> cookieMap;

}
