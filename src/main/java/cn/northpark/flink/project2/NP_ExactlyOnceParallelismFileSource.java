package cn.northpark.flink.project2;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.Iterator;

public class NP_ExactlyOnceParallelismFileSource extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {

    private String path;

    private boolean flag = true;

    private Long offset = 0L;

    private transient  ListState<Long> offsetState;

    public NP_ExactlyOnceParallelismFileSource(String path) {
        this.path = path;
    }

    public NP_ExactlyOnceParallelismFileSource() {
    }

    @Override
    public void run(SourceContext ctx) throws Exception {

        Iterator<Long> iterator = offsetState.get().iterator();
        while (iterator.hasNext()){
           offset = iterator.next();
        }

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        RandomAccessFile file = new RandomAccessFile(path + "/" + subtaskIndex + ".txt", "r");

        file.seek(offset);

        Object lock = ctx.getCheckpointLock();

        while (flag) {
            String readLine = file.readLine();
            if (readLine != null) {
                readLine = new String(readLine.getBytes("ISO-8859-1"),"UTF-8" );
                synchronized (lock){
                    offset = file.getFilePointer();
                    ctx.collect(Tuple2.of(subtaskIndex + "", readLine));
                }
            }else {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;

    }

    /**
     * 快照 ：周期执行
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //clear
        offsetState.clear();
        //set
        offsetState.add(offset);
    }

    /**
     * 初始化operator state，构造方法执行以后会执行一次
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<Long>("opeartor-state-np", Types.LONG);
        //定义一个zh状态描述器
        offsetState = context.getOperatorStateStore().getListState(listStateDescriptor);
    }
}
