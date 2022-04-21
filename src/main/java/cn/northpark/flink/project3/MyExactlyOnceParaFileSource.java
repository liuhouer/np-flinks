package cn.northpark.flink.project3;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

public class MyExactlyOnceParaFileSource extends RichParallelSourceFunction<Tuple2<String,String>> implements CheckpointedFunction {
    private String path;

    private boolean flag = true;

    private transient ListState<Long> offsetState;

    private Long offset = 0L;

    public MyExactlyOnceParaFileSource(String path) {
        this.path = path;
    }

    public MyExactlyOnceParaFileSource() {
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        Iterable<Long> iterable = offsetState.get();
        while(iterable.iterator().hasNext()){
            offset = offsetState.get().iterator().next();
        }

        int index = getRuntimeContext().getIndexOfThisSubtask();

        RandomAccessFile randomAccessFile = new RandomAccessFile(path +"/" +index +".txt","r");

        randomAccessFile.seek(offset);

        final  Object lock = ctx.getCheckpointLock();

        while (flag){
            String line = randomAccessFile.readLine();
            if(line!=null){
                line = new String(line.getBytes("ISO-8859-1"),"utf-8");
                synchronized (lock){
                    offset = randomAccessFile.getFilePointer();
                    ctx.collect(Tuple2.of(index+"",line));
                }
            }else{
                Thread.sleep(2000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = true;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //clear
        offsetState.clear();

        //set offset
        offsetState.add(offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<Long>("np-operator-state",
                    TypeInformation.of(new TypeHint<Long>() {
                    })
//                Types.LONG
//                Long.class
        );
       offsetState = context.getOperatorStateStore().getListState(stateDescriptor);
    }
}
