package cn.northpark.flink.rrys;

import cn.northpark.flink.util.JsonUtil;
import cn.northpark.flink.util.TimeUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author bruce
 * @date 2021年11月25日 16:48:32
 */
public class AnaRRDY {
    public static void main(String[] args) {
        List<Map<String,Object>> maps = new ArrayList<>();
        maps.stream().parallel().unordered().forEach(item -> {
            Map<String, String> map = Maps.newConcurrentMap();

            //基本信息获取
            Object cnname = item.get("cnname");
            Object enname = item.get("enname");
            Object aliasname = item.get("aliasname");

            String dataStr = item.get("data").toString();
            Map<String, Object> dataMap = JsonUtil.json2map(dataStr);

            String data_inner = dataMap.get("data").toString();

            Map<String, Object> data_inner_map = JsonUtil.json2map(data_inner);


            //"info": {
            //		"area": "美国",
            //		"show_type": "",
            //		"aliasname": "",
            //		"year": [2012],
            //		"cnname": "星际旅行 动画版",
            //		"expire": "1610393635",
            //		"channel": "tv",
            //		"id": 26486,
            //		"channel_cn": "美剧",
            //		"views": 6,
            //		"enname": "Star Trek The Animated"
            //	}
            String d_info = data_inner_map.get("info").toString();
            Map<String, Object> d_info_map = JsonUtil.json2map(d_info);
            Object area = d_info_map.get("area");
            Object year = d_info_map.get("year");
            Object channel_cn = d_info_map.get("channel_cn");
            String expire_date = TimeUtils.nowdate();
            if (Objects.nonNull(d_info_map.get("expire"))) {
                //timestamp
                expire_date = TimeUtils.getHalfDate(TimeUtils.Timestamp2DateStr(Long.parseLong(d_info_map.get("expire").toString())));
            }

            //下载资源解析
            StringBuilder sb_down = new StringBuilder();
            List<Map<String, Object>> listMap = JsonUtil.json2ListMap(data_inner_map.get("list").toString());
            for (Map<String, Object> item_ : listMap) {
                //不同格式列表
                JSONArray formats = JSON.parseArray(item_.get("formats").toString());
                Object down_name = item_.get("season_cn");
                sb_down.append("<div class='left'>");
                sb_down.append("<p>").append(down_name).append("</p>");
                Map<String, Object> formatsMap = JsonUtil.json2map(item_.get("items").toString());
                for (Object format : formats) {

                    sb_down.append("<p>").append("<bold class='text-danger'>").append(format).append("</bold>").append("</p>");
                    List<Map<String, Object>> format_downs = JsonUtil.json2ListMap(formatsMap.get(format).toString());

                    for (Map<String, Object> format_down : format_downs) {
                        Object name = format_down.get("name");
                        Object size = format_down.get("size");
                        sb_down.append("<p>").append(name).append("-").append(size).append("</p>");

                        if (Objects.isNull(format_down.get("files"))) {
                            continue;
                        }
                        JSONArray files = JSON.parseArray(format_down.get("files").toString());

                        for (Object obj : files) {
                            //"address": "magnet:?xt=urn:btih:HFS7OB6KVE452C4LTCJ6H6AIOHCKPION&tr.0=http://idowns.org:6969/announce&tr.1=http://idowns.org:2710/announce&tr.2=http://tracker.openbittorrent.com/announce&tr.3=udp://tracker.openbittorrent.com:80/announce&tr.4=http://tracker.thepiratebay.org/announce&tr.5=http://tracker.publicbt.com/announce",
                            //			"passwd": "",
                            //			"way_cn": "磁力",
                            //			"way": "2"
                            Map<String, Object> file = JsonUtil.json2map(obj.toString());
                            Object address = file.get("address");
                            Object passwd = file.get("passwd");
                            Object way_cn = file.get("way_cn");

                            if (Objects.nonNull(way_cn) && StringUtils.isNotEmpty(way_cn.toString())) {
                                sb_down.append("<p>").append("下载方式：")
                                        .append("<a href='"+address+"' target='_blank'>")
                                        .append("<span class='text-primary'>").append(way_cn).append("</span>").append("</p>")
                                        .append("</a>");
                            }
                            if (Objects.nonNull(passwd) && StringUtils.isNotEmpty(passwd.toString())) {
                                sb_down.append("<p>").append("密码：").append(passwd).append("</p>");
                            }


                        }

                        //{
                        //		"itemid": "65051",
                        //		"size": "5GB",
                        //		"dateline": "1336855335",
                        //		"yyets_trans": 0,
                        //		"name": "危情谍战.Knight.and.Day.2010.Bluray.720p.DTS.x264-CHD.mkv  5 GB",
                        //		"files": [{
                        //			"address": "ed2k://|file|[危情谍战].Knight.and.Day.2010.Bluray.720p.DTS.x264-CHD.mkv|5368388375|08c33e07c02998268a450b0a45fb45e1|h=kuavcoflnpjnjwfq7uh7okmcovlhihwi|/",
                        //			"passwd": "",
                        //			"way_cn": "电驴",
                        //			"way": "1"
                        //		}],
                        //		"episode": "0"
                    }
                }

            }

            sb_down.append("</div>");
            System.err.println(sb_down.toString());
            System.err.println("**************************************");


            String title = cnname + "-" + enname;
            String tag = channel_cn + "," + area + "," + year;
            StringBuilder sb_article = new StringBuilder();
            //"info": {
            //                //		"area": "美国",
            //                //		"show_type": "",
            //                //		"aliasname": "",
            //                //		"year": [2012],
            //                //		"cnname": "星际旅行 动画版",
            //                //		"expire": "1610393635",
            //                //		"channel": "tv",
            //                //		"id": 26486,
            //                //		"channel_cn": "美剧",
            //                //		"views": 6,
            //                //		"enname": "Star Trek The Animated"
            //                //	}
            sb_article.append("<p>").append("中文名：").append(cnname).append("</p>");
            sb_article.append("<p>").append("英文名：").append(enname).append("</p>");
            sb_article.append("<p>").append("别名：").append(aliasname).append("</p>");
            sb_article.append("<p>").append("地区：").append(area).append("</p>");
            sb_article.append("<p>").append("年份：").append(year).append("</p>");
            sb_article.append("<p>").append("收录时间：").append(expire_date).append("</p>");
            map.put("title", title);
            map.put("a_url", "");
            map.put("date", expire_date);
            map.put("article", sb_article.toString());
//            map.put("ret_code", MD5Utils.encrypt(title, MD5Utils.MD5_KEY));
            map.put("tag", tag);
//            map.put("tag_code", PinyinUtil.paraseStringToPinyin(tag).toLowerCase());
            map.put("path", sb_down.toString());

//            list.add(map);

        });
    }
}
