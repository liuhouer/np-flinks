package cn.northpark.flink.topN.spark.passwordStasApp;

import java.util.StringTokenizer;

/**
 * @author bruce
 * @date 2022年06月25日 12:21:23
 */
public class Test1 {
    public static void main(String[] args) {
        String exp = "shibazi_lin@126.com\t6584596";
        // 去掉所有的键盘上的不可输入字符，不包括双字节的，32-126
        String pattern = "[^\040-\176]";
        String line = exp.toString().replaceAll(pattern, " ");
        StringTokenizer itr = new StringTokenizer(line);

        while (itr.hasMoreTokens()) {
            System.err.println(itr.nextToken());
        }
    }
}
