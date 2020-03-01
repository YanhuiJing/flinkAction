package flinkstate;

import flinkconnector.Tuple2Source;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/27
 * @description：
 * 1,广播流不断进行状态累积,可以等同理解为sparkStraming动态更新广播变量(但是要比sparkStreaming灵活很多)
 * 2,数据流获取广播变量进行对应的逻辑处理
 */
public class BroadCastStateAction {

    // 定义广播变量状态描述
    public static final MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("broadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<String>() {
    }));

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> broadcastSource = executionEnvironment.addSource(new BroadCastSource(5));

        //广播广播流
        BroadcastStream<String> broadcast = broadcastSource.broadcast(mapStateDescriptor);

        executionEnvironment.addSource(new Tuple2Source(10))
         .connect(broadcast)
         .process(new BroadcastProcessFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>() {

             //处理数据流,数据流获取广播变量,进行对应的逻辑处理
             @Override
             public void processElement(Tuple2<String, Integer> value, ReadOnlyContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                 ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                 if(Objects.nonNull(broadcastState.get(value.f0))){
                     out.collect(value);
                 }

             }

             //处理广播流,将广播流中的元素添加到广播状态中,供其他数据流进行使用
             @Override
             public void processBroadcastElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                 BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                 broadcastState.put(value,value);

             }
         }).print();


         executionEnvironment.execute("broadcastState");


    }

    public static class BroadCastSource extends RichSourceFunction<String>{

        private volatile boolean isRunning = true;

        private List<String> tuple2List = Arrays.asList(

                "a","b","c"

        );

        private int count;

        public BroadCastSource(int count){

            this.count = count;

        }


        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            while(isRunning){

                for(int i=0;i<count;i++){
                    for(String tuple:tuple2List){

                        Thread.sleep(2000);
                        ctx.collect(tuple);

                    }
                }


            }

        }

        @Override
        public void cancel() {

            isRunning = false;

        }
    }


}
