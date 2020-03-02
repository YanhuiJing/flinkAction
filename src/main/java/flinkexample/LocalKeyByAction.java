package flinkexample;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 使用OperState完成localBykey解决数据倾斜问题
 *
 * @author gavin
 * @createDate 2020/3/1
 *
 * flink内置shuffle算子
 * dataStream.shuffle();	将数据随机地分配到下游 Operator 实例
 * dataStream.rebalance();	使用轮循的策略将数据发送到下游 Operator 实例
 * dataStream.rescale();	基于 rebalance 优化的策略，依然使用轮循策略，但仅仅是 TaskManager 内的轮循，只会在 TaskManager 本地进行 shuffle 操作，减少了网络传输
 */
public class LocalKeyByAction extends RichFlatMapFunction<String, Tuple2<String,Long>> implements CheckpointedFunction {

    //Checkpoint 时为了保证 Exactly Once，将 buffer 中的数据保存到该 ListState 中
    private ListState<Tuple2<String, Long>> localPvStatListState;

    //本地 buffer，存放 local 端缓存的 app 的 pv 信息
    private HashMap<String, Long> localPvStat;

    //缓存的数据量大小，即：缓存多少数据再向下游发送
    private int batchSize;

    //计数器，获取当前批次接收的数据量
    private AtomicInteger currentSize;

    public LocalKeyByAction(int batchSize){

        this.batchSize = batchSize;

    }


    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {

        Long count = localPvStat.getOrDefault(value, 0L);

        localPvStat.put(value,count+1);

        if(currentSize.incrementAndGet() >= batchSize){

            for(Map.Entry<String,Long> entry:localPvStat.entrySet()){
                out.collect(Tuple2.of(entry.getKey(),entry.getValue()));
            }

            localPvStat.clear();
            currentSize.set(0);

        }


    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        localPvStatListState.clear();

        for(Map.Entry<String,Long> entry:localPvStat.entrySet()){
            localPvStatListState.add(Tuple2.of(entry.getKey(),entry.getValue()));
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor listStateDescriptor = new ListStateDescriptor(
                "localByKeyState",
                TypeInformation.of(new TypeHint<Tuple2<String,Long>>() {
                })
        );

        //每个算子中的Managed Operator State都是以List形式存储,算子和算子之间的状态数据相互独立,List存储比较
        //适合于状态数据的重新分布,Flink目前支持Event-split Redistribution 和 Union Redistribution
        //Event-split Redistribution
        //     每个算子实例中含有部分状态元素的list列表,当触发restore和redistribution动作时,通过将状态数据平
        //  均分配成与算子并行度相同数量的List列表,每个task实例中有一个list,其中可以为空或者含有多个元素;
        //  通过context.getOperatorStateStore().getListState(listStateDescriptor)获取状态
        //Union Redistribution
        //      每个算子实例中包含所有状态元素的List列表,当触发restore和redistribution动作时,每个算子都能够
        //获取完整的状态元素列表
        //
        localPvStatListState= context.getOperatorStateStore().getListState(listStateDescriptor);

        localPvStat = new HashMap<>();

        if(context.isRestored()){
            // 从状态中恢复数据到 localPvStat 中
            for(Tuple2<String,Long> tuple:localPvStatListState.get()){

                Long orDefault = localPvStat.getOrDefault(tuple.f0, 0L);
                localPvStat.put(tuple.f0,tuple.f1+orDefault);

            }

            //  从状态恢复时，默认认为 buffer 中数据量达到了 batchSize，需要向下游发送数据了
            currentSize = new AtomicInteger(batchSize);

        }else{

            currentSize = new AtomicInteger(0);
        }

    }


}
