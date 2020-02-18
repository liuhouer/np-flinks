package cn.northpark.flink.project3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

public class MyParaFileSource extends RichParallelSourceFunction<Tuple2<String,String>> {
    private String path;

    private boolean flag = true;

    public MyParaFileSource(String path) {
        this.path = path;
    }

    public MyParaFileSource() {
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        int index = getRuntimeContext().getIndexOfThisSubtask();

        RandomAccessFile randomAccessFile = new RandomAccessFile(path +"/" +index +".txt","r");

        while (flag){
            String line = randomAccessFile.readLine();
            if(line!=null){
                line = new String(line.getBytes("ISO-8859-1"),"utf-8");
                ctx.collect(Tuple2.of(index+"",line));
            }else{
                Thread.sleep(2000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = true;
    }
}
