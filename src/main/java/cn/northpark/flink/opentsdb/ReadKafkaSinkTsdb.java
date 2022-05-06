package cn.northpark.flink.opentsdb;

import cn.northpark.flink.bean.StatisticsVO;
import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author bruce
 * @date 2022年5月6日 09:34:28
 * 根据应用日志分析url信息 |用户请求信息 ......
 *
 * 读取kafka进行数据分析写入openTSDB
 *
 * 应用日志导入到kafka
 * <pre>
 * [np_web][INFO] [2022-04-06 17:45:38] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.listPage","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/page/1343"}
 * [np_web][INFO] [2022-04-06 17:45:43] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/512083/page/0"}
 * [np_web][INFO] [2022-04-06 17:45:43] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-681803.html"}
 * [np_web][INFO] [2022-04-06 17:45:46] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-685722.html"}
 * [np_web][INFO] [2022-04-06 17:46:04] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-721984.html"}
 * [np_web][INFO] [2022-04-06 17:46:05] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-687446.html"}
 * [np_web][INFO] [2022-04-06 17:46:23] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-682568.html"}
 * [np_web][INFO] [2022-04-06 17:46:35] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.fans","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/cm/fans/511836"}
 * [np_web][INFO] [2022-04-06 17:46:36] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.SoftAction.monthSearch","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/soft/month/2019-02/page/4"}
 * [np_web][INFO] [2022-04-06 17:46:40] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotes","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/510764"}
 * [np_web][INFO] [2022-04-06 17:46:41] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/510764/page/1"}
 * [np_web][INFO] [2022-04-06 17:46:58] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.PoemAction.detail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/poem/enjoy/570352.html"}
 * [np_web][INFO] [2022-04-06 17:47:17] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.people","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/people/yauo200926"}
 * [np_web][INFO] [2022-04-06 17:47:20] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.LyricsAction.comment","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/love/Gmail.html"}
 * [np_web][INFO] [2022-04-06 17:47:32] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.PoemAction.detail","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/poem/enjoy/574106.html"}
 * [np_web][INFO] [2022-04-06 17:47:35] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.PoemAction.detail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/poem/enjoy/572868.html"}
 * [np_web][INFO] [2022-04-06 17:47:41] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/taiwan/page/36"}
 * [np_web][INFO] [2022-04-06 17:47:53] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-736031.html"}
 * [np_web][INFO] [2022-04-06 17:48:07] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.people","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/people/715029925"}
 * [np_web][INFO] [2022-04-06 17:48:11] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.people","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/people/sheghuimi11594522528"}
 * [np_web][INFO] [2022-04-06 17:48:20] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.LyricsAction.comment","cookieMap":{"UA":"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/love/dai-fa-xiang-ji-9097.html"}
 * [np_web][INFO] [2022-04-06 17:48:29] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.fans","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/cm/fans/511089"}
 * [np_web][INFO] [2022-04-06 17:48:31] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"YisouSpider"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-681841.html"}
 * [np_web][INFO] [2022-04-06 17:48:37] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.SoftAction.softDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/soft/fontexplorer-x-pro-748.html"}
 * [np_web][INFO] [2022-04-06 17:48:38] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/513819/page/4"}
 * [np_web][INFO] [2022-04-06 17:48:48] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-718342.html"}
 * [np_web][INFO] [2022-04-06 17:49:07] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.PoemAction.detail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/poem/enjoy/563538.html"}
 * [np_web][INFO] [2022-04-06 17:49:18] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 10; LYA-L29) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.79 Mobile Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-723797.html"}
 * [np_web][INFO] [2022-04-06 17:49:23] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.fans","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/cm/fans/517723"}
 * [np_web][INFO] [2022-04-06 17:49:25] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-704771.html"}
 * [np_web][INFO] [2022-04-06 17:49:29] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotes","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/516813"}
 * [np_web][INFO] [2022-04-06 17:49:30] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/516813/page/1"}
 * [np_web][INFO] [2022-04-06 17:49:37] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/512083/page/1"}
 * [np_web][INFO] [2022-04-06 17:49:39] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/kongbu/page/140"}
 * [np_web][INFO] [2022-04-06 17:49:42] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-673012.html"}
 * [np_web][INFO] [2022-04-06 17:50:00] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.fans","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/cm/fans/511582"}
 * [np_web][INFO] [2022-04-06 17:50:18] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotes","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/518190"}
 * [np_web][INFO] [2022-04-06 17:50:19] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/518190/page/1"}
 * [np_web][INFO] [2022-04-06 17:50:26] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/hanguo/page/111"}
 * [np_web][INFO] [2022-04-06 17:50:36] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-672021.html"}
 * [np_web][INFO] [2022-04-06 17:50:44] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.people","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/people/lovedepp520"}
 * [np_web][INFO] [2022-04-06 17:50:44] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-750062.html"}
 * [np_web][INFO] [2022-04-06 17:50:54] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-671398.html"}
 * [np_web][INFO] [2022-04-06 17:51:13] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/danstevens/page/1"}
 * [np_web][INFO] [2022-04-06 17:51:27] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/513083/page/4"}
 * [np_web][INFO] [2022-04-06 17:51:27] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-747271.html"}
 * [np_web][INFO] [2022-04-06 17:51:30] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.fans","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/cm/fans/511604"}
 * [np_web][INFO] [2022-04-06 17:52:06] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-717172.html"}
 * [np_web][INFO] [2022-04-06 17:52:07] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-769546.html"}
 * [np_web][INFO] [2022-04-06 17:52:09] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-768887.html"}
 * [np_web][INFO] [2022-04-06 17:52:17] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/513525/page/2"}
 * [np_web][INFO] [2022-04-06 17:52:25] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-756802.html"}
 * [np_web][INFO] [2022-04-06 17:52:43] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-767716.html"}
 * [np_web][INFO] [2022-04-06 17:52:43] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.people","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/people/65380154014"}
 * [np_web][INFO] [2022-04-06 17:52:44] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.LyricsAction.comment","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/love/caimai.html"}
 * [np_web][INFO] [2022-04-06 17:52:56] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.SoftAction.softDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/soft/a-better-finder-rename-495.html"}
 * [np_web][INFO] [2022-04-06 17:52:56] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/515053/page/3"}
 * [np_web][INFO] [2022-04-06 17:53:01] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-771710.html"}
 * [np_web][INFO] [2022-04-06 17:53:18] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.SoftAction.monthSearch","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/soft/month/2013-10/page/6"}
 * [np_web][INFO] [2022-04-06 17:53:19] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.listPage","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/page/7729"}
 * [np_web][INFO] [2022-04-06 17:53:19] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.LyricsAction.comment","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/love/once.html"}
 * [np_web][INFO] [2022-04-06 17:53:37] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.fans","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/cm/fans/511658"}
 * [np_web][INFO] [2022-04-06 17:53:42] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-724139.html"}
 * [np_web][INFO] [2022-04-06 17:53:55] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/qita/page/16"}
 * [np_web][INFO] [2022-04-06 17:54:14] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-691669.html"}
 * [np_web][INFO] [2022-04-06 17:54:31] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-772053.html"}
 * [np_web][INFO] [2022-04-06 17:54:33] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-747033.html"}
 * [np_web][INFO] [2022-04-06 17:54:33] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/riju/page/63"}
 * [np_web][INFO] [2022-04-06 17:54:37] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/512420/page/4"}
 * [np_web][INFO] [2022-04-06 17:54:44] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/510691/page/4"}
 * [np_web][INFO] [2022-04-06 17:54:44] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/511871/page/4"}
 * [np_web][INFO] [2022-04-06 17:54:50] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/dalu/page/213"}
 * [np_web][INFO] [2022-04-06 17:55:08] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-763546.html"}
 * [np_web][INFO] [2022-04-06 17:55:23] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.people","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/people/453755097"}
 * [np_web][INFO] [2022-04-06 17:55:26] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/dalu/page/220"}
 * [np_web][INFO] [2022-04-06 17:55:42] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.SoftAction.softDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/soft/imageranger-pro-441.html"}
 * [np_web][INFO] [2022-04-06 17:55:44] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/jilupian/page/56"}
 * [np_web][INFO] [2022-04-06 17:56:02] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-766017.html"}
 * [np_web][INFO] [2022-04-06 17:56:10] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.people","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/people/25876341130"}
 * [np_web][INFO] [2022-04-06 17:56:10] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.people","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/people/93242386402"}
 * [np_web][INFO] [2022-04-06 17:56:14] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/510650/page/3"}
 * [np_web][INFO] [2022-04-06 17:56:20] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/jiaju/page/3"}
 * [np_web][INFO] [2022-04-06 17:56:38] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-761685.html"}
 * [np_web][INFO] [2022-04-06 17:56:44] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotes","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/517797"}
 * [np_web][INFO] [2022-04-06 17:56:45] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.LyricsAction.comment","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/love/bing-dang.html"}
 * [np_web][INFO] [2022-04-06 17:56:45] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/517797/page/1"}
 * [np_web][INFO] [2022-04-06 17:56:56] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-768934.html"}
 * [np_web][INFO] [2022-04-06 17:57:14] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.UserAction.fans","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/cm/fans/513695"}
 * [np_web][INFO] [2022-04-06 17:57:15] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-756909.html"}
 * [np_web][INFO] [2022-04-06 17:57:29] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-740523.html"}
 * [np_web][INFO] [2022-04-06 17:57:30] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.NoteAction.viewNotesPages","cookieMap":{"JSESSIONID":"9633258506AA40889BEC08B411A06085","UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/note/viewNotes/510679/page/3"}
 * [np_web][INFO] [2022-04-06 17:57:33] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-712404.html"}
 * [np_web][INFO] [2022-04-06 17:57:40] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-729835.html"}
 * [np_web][INFO] [2022-04-06 17:57:51] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/qingse/page/2127"}
 * [np_web][INFO] [2022-04-06 17:58:09] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-768596.html"}
 * [np_web][INFO] [2022-04-06 17:58:10] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/bilishi/page/2"}
 * [np_web][INFO] [2022-04-06 17:58:10] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-722798.html"}
 * [np_web][INFO] [2022-04-06 17:58:27] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/fanzui/page/154"}
 * [np_web][INFO] [2022-04-06 17:58:38] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.postDetail","cookieMap":{"UA":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 YisouSpider/5.0 Safari/537.36"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/post-689163.html"}
 * [np_web][INFO] [2022-04-06 17:58:45] cn.northpark.aspect.HttpAspect.log(140) | [Statistics Info]^{"class_method":"cn.northpark.action.MoviesAction.tag_list_page","cookieMap":{"UA":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"},"ip":"172.17.0.2","method":"GET","url":"http://northpark.cn/movies/tag/juqing/page/1272"}
 * </pre>
 */
@Slf4j
public class ReadKafkaSinkTsdb {

    public static void main(String[] args) throws Exception {

        //1.环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //load config infos
        InputStream is = ReadKafkaSinkTsdb.class.getClassLoader().getResourceAsStream("kafka.properties");

        ParameterTool parameters = ParameterTool.fromPropertiesFile(is);


        //###############定义消费kafka source##############
        Properties props = new Properties();
        props.put("bootstrap.servers",parameters.getRequired("bootstrap.servers"));
        //props.put("zookeeper.connect", parameters.getRequired("zookeeper.connect"));
        props.put("group.id", parameters.getRequired("group.id"));
        props.put("key.deserializer", parameters.getRequired("key.deserializer"));
        props.put("value.deserializer", parameters.getRequired("value.deserializer"));
        props.put("auto.offset.reset", parameters.getRequired("auto.offset.reset"));

        //2.read source
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(parameters.getRequired("topics"), new SimpleStringSchema(), props);


        DataStream<String> lines = env.addSource(kafkaSource);

        //3.transform-转化为实体
        SingleOutputStreamOperator<StatisticsVO> map = lines.map(new MapFunction<String, StatisticsVO>() {

            @Override
            public StatisticsVO map(String value) throws Exception {
                int start_index = value.indexOf("[Statistics Info]^");
                String replace_1 = value.substring(start_index).replace("[Statistics Info]^", "");

                StatisticsVO vo = JSON.parseObject(replace_1, StatisticsVO.class);

                //替换tsdb中的违规字符
                vo.url = vo.url.replace(":","-").replace("%","-").replace("(","-").replace(")","-");

                log.info("log--url----{}",vo.url);

                return vo;
            }

        }).filter(new FilterFunction<StatisticsVO>() {
            @Override
            public boolean filter(StatisticsVO value) throws Exception {
                return value.url != null;
            }
        });


        //transform--统计url和出现的次数---

        map.flatMap(new FlatMapFunction<StatisticsVO, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(StatisticsVO value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value.url, 1));
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1).addSink(new RichSinkFunction<Tuple2<String, Integer>>() {

            private HiTSDB opentsdb = null;
            private HiTSDBConfig config;

            /**
             * 创建连接
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                config = HiTSDBConfig
                        .address("node1", 4242)
                        .readonly(false)
                        .asyncPut(true)
                        .listenBatchPut(new BatchPutCallback() {
                            @Override
                            public void response(String address, List<Point> input, Result output) {
                                log.info("success save into openTSDB, data size:" + input.size());
                            }

                            @Override
                            public void failed(String address, List<Point> input, Exception ex) {
                                log.error(ex.getMessage(), ex);
                                log.error("fail to save data into OpenTSDB:" + input.size());
                            }
                        })
                        .config();
                opentsdb = HiTSDBClientFactory.connect(config);
            }

            /**
             * 关闭连接
             * @throws Exception
             */
            @Override
            public void close() throws Exception {
                super.close();
                if (opentsdb != null) {
                    opentsdb.close(true);
                }
            }

            //写入opentsdb
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                List<Point> points = new ArrayList<>();
                Point highPoint = Point.metric("metric").tag("type", value.f0).value(System.currentTimeMillis(), value.f1).build();
                points.add(highPoint);

                DetailsResult detailsResult = opentsdb.putSync(points, DetailsResult.class);
                if (detailsResult.getFailed() > 0) {
                    log.error("Put records into openTSDB failed, count: {}", detailsResult.getFailed());
                    log.error(detailsResult.getErrors().toString());
                }
            }
        });

        //4.execute
        env.execute("ReadKafkaSinkTsdb");

    }
}
