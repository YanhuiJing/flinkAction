package flinkstate;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/27
 * @description： TODO
 */
public class OperatorStateAction implements SinkFunction<Tuple2<String,Long>>, CheckpointedFunction {

    private final int threshold;

    private transient ListState<Tuple2<String, Long>> checkpointedState;

    private List<Tuple2<String, Long>> bufferedElements;

    public OperatorStateAction(int threshold) {

        this.threshold = threshold;
        bufferedElements = new ArrayList<>();

    }


    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        bufferedElements.add(value);

        if(bufferedElements.size() == threshold){
            for(Tuple2<String,Long> bufferedElement : bufferedElements){
                // 数据处理逻辑,可以将数据发送到mysql等存储系统
            }
            bufferedElements.clear();
        }

    }

    // 当有请求执行checkpoint()时,snapshot()方法就会被调用
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        checkpointedState.clear();
        for(Tuple2<String,Long> bufferedElement : bufferedElements){
            checkpointedState.add(bufferedElement);
        }

    }

    // initializeState会在每次初始化用户的函数或者从更早的checkpoint恢复时被调用,恢复state的逻辑和数据
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor listStateDescriptor = new ListStateDescriptor(
                "listState",
                TypeInformation.of(new TypeHint<Tuple2<String,Long>>() {
                }));

        checkpointedState = context.getOperatorStateStore().getListState(listStateDescriptor);

        if(context.isRestored()){

            for(Tuple2<String,Long> bufferedElement : checkpointedState.get()){
                bufferedElements.add(bufferedElement);
            }

        }


    }
}
