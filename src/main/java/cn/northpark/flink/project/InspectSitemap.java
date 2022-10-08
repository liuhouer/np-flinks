package cn.northpark.flink.project;

import cn.northpark.flink.util.HttpGetUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * @author bruce
 * @date 2022年10月08日 13:48:34
 */
public class InspectSitemap {
    public static void main(String[] args)  throws  Exception{

        //dataSet api
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSource<String> sitemap_lines = env.readTextFile("C:\\Users\\Bruce\\Downloads\\soft.txt");

        MapOperator<String, Object> bad_url = sitemap_lines.map(new RichMapFunction<String, Object>() {

            @Override
            public Object map(String value) throws Exception {


                String dataResult = HttpGetUtils.getDataResult(value);

                if (StringUtils.isBlank(dataResult)) {
                   return value;
                }

                return null;
            }

        });

        bad_url.writeAsText("C:\\Users\\Bruce\\Downloads\\bad_req.txt");

        env.execute("aa");
    }
}
