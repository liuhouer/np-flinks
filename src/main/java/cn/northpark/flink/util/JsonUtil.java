package cn.northpark.flink.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class JsonUtil {

	//以下是常用的方法==========================================================================================================

	/**
	 * object to json
	 *
	 * @param object
	 *          the object that will transform to json string
	 * @return
	 *          the json string of object
	 */
	public static String object2json(Object object) {
		return JSON.toJSONString(object);
	}
	
	/**
	 * object to json
	 *
	 * @param object
	 *          the object that will transform to json string
	 * @return
	 *          the json string of object
	 */
	public static String object2jsonWriteNullValue(Object object) {
		return JSON.toJSONString(object,SerializerFeature.PrettyFormat,SerializerFeature.WriteMapNullValue,SerializerFeature.WriteNullListAsEmpty,SerializerFeature.WriteNullStringAsEmpty,SerializerFeature.WriteBigDecimalAsPlain);
	}


	/**
	 * json to list
	 *
	 * @param json
	 *          the json string that will transform to list
	 * @param clazz
	 *          the class of the list's element
	 * @param <T>
	 *          the generic of the class
	 * @return
	 *          the list that json string transform
	 */
	public static <T> List<T> json2list(String json, Class<T> clazz) {
		return JSON.parseArray(json, clazz);
	}
	

	/**
	 * json to map
	 *
	 * @param json
	 *          json string that will transform to map
	 * @return
	 *          the map fo json string
	 */
	public static Map<String,Object> json2map(String json) {
		return JSONObject.parseObject(json);
	}


	/**
	 * json string to object array
	 *
	 * @param json
	 *          the json string will transform to object array
	 * @param clazz
	 *          the class of the json will transform
	 * @param ts
	 *          the real object array
	 * @param <T>
	 *          the real object
	 * @return
	 *          the object array of the json string
	 *
	 * @param json
	 * @param clazz
	 * @param ts
	 * @param <T>
	 * @return
	 */
	public static <T> T[] json2array(String json, Class<T> clazz, T[] ts) {
		return JSON.parseArray(json, clazz).toArray(ts);
	}

	/**
	 * 用fastjson 将json字符串解析为一个 JavaBean
	 * 
	 * @param jsonString
	 * @param cls
	 * @return
	 */
	public static <T> T json2object(String jsonString, Class<T> cls) {
		T t = null;
		try {
			t = JSON.parseObject(jsonString, cls);
		} catch (Exception e) {

			e.printStackTrace();
		}
		return t;
	}

	
	/**
	 * 用fastjson 将jsonString 解析成 List<Map<String,Object>>
	 * 
	 * @param jsonString
	 * @return
	 */
	public static List<Map<String, Object>> json2ListMap(String jsonString) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			// 两种写法
			// list = JSON.parseObject(jsonString, new
			// TypeReference<List<Map<String, Object>>>(){}.getType());

			list = JSON.parseObject(jsonString, new TypeReference<List<Map<String, Object>>>() {
			});
		} catch (Exception e) {

			e.printStackTrace();
		}
		return list;
	}

	//以上是常用的方法==========================================================================================================

	public static void main(String[] args) {
		log.error("some thing");
	}
}
