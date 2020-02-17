package cn.northpark.flink.project2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

public class NP_ParallelismFileSource extends RichParallelSourceFunction<Tuple2<String, String>> {

    private String path;

    private boolean flag = true;

    public NP_ParallelismFileSource(String path) {
        this.path = path;
    }

    public NP_ParallelismFileSource() {
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        RandomAccessFile file = new RandomAccessFile(path + "/" + subtaskIndex + ".txt", "r");


        while (flag) {
            String readLine = file.readLine();
            if (readLine != null) {
                readLine = new String(readLine.getBytes("ISO-8859-1"),"UTF-8" );
                ctx.collect(Tuple2.of(subtaskIndex + "", readLine));
            }else {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;

    }
}
