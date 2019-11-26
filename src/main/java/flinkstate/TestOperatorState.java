package flinkstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * operatorState案例
 *
 * @author gavin
 * @createDate 2019/11/26
 */
public class TestOperatorState {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置checkpoint
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConfig=env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setFailOnCheckpointingErrors(true);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        MemoryStateBackend memoryBackend = new MemoryStateBackend(10*1024*1024);

        env.setStateBackend(memoryBackend);


        DataStream<Long> inputStream=env.fromElements(1L,2L,3L,4L,5L,1L,3L,4L,5L,6L,7L,1L,4L,5L,3L,9L,9L,2L,1L);

        inputStream.flatMap(new CountWithOperatorState())
                .setParallelism(1)
                .print();

        env.execute();

    }
}


/**
 *
 *
 * 想知道两次事件1之间，一共发生多少次其他事件，分别是什么事件
 *
 * 事件流：1 2 3 4 5 1 3 4 5 6 7 1 4 5 3 9 9 2 1...
 * 输出：
 *   (4,2 3 4 5)
 *      (5,3 4 5 6 7)
 *      (6,4 5 6 9 9 2)
 */
class CountWithOperatorState extends RichFlatMapFunction<Long,Tuple2<Integer,String>> implements CheckpointedFunction {
    /**
     * 托管状态
     */
    private transient ListState<Long> checkPointCountList;
    /**
     * 原始状态
     */
    private List<Long> listBufferElements;
    @Override
    public void flatMap(Long value, Collector<Tuple2<Integer,String>> out) throws Exception {
        if(value == 1){
            if(listBufferElements.size()>0){
                StringBuffer buffer=new StringBuffer();
                for(Long item:listBufferElements){
                    buffer.append(item+" ");
                }
                out.collect(Tuple2.of(listBufferElements.size(),buffer.toString()));
                listBufferElements.clear();
            }
        }else{
            listBufferElements.add(value);
        }
    }

    /**
     * 做checkpoint的时候对数据做快照
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkPointCountList.clear();
        for(Long item:listBufferElements){
            checkPointCountList.add(item);
        }
    }

    /**
     * 重启恢复数据
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor=
                new ListStateDescriptor<Long>("checkPointCountList", TypeInformation.of(new TypeHint<Long>() {}));
        checkPointCountList=context.getOperatorStateStore().getListState(listStateDescriptor);
        if(context.isRestored()){
            for(Long element:checkPointCountList.get()){
                listBufferElements.add(element);
            }
        }
    }

    /**
     * 对原始状态做初始化
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        listBufferElements=new ArrayList<>();
    }
}
