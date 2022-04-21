package cn.northpark.flink.scala.suicideApp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

/**
 * ！！！hadoop hdfs连接不上 直接把9000端口去掉就可以了！！！！
 * @author bruce
 * @date 2022年04月21日 09:51:08
 */
public class HdfsTest {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        //这里设置namenode
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.nameservices", "node1");
        conf.set("fs.defaultFS", "hdfs://node1");
        FileSystem fileSystem1 = FileSystem.get(conf);
        System.out.println("===contains1===");
        //测试访问情况
        Path path=new Path("/scd");
        System.out.println("===contains2===");
        if(fileSystem1.exists(path)){
            System.out.println("===contains3===");
        }
        System.out.println("===contains4===");
        RemoteIterator<LocatedFileStatus> list=fileSystem1.listFiles(path,true);
        while (list.hasNext()){
            LocatedFileStatus fileStatus=list.next();
            System.out.println(fileStatus.getPath());
        }
    }
}
