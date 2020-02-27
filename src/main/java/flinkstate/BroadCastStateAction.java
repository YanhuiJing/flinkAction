package flinkstate;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/27
 * @description： example未完成
 */
public class BroadCastStateAction {

    public static void main(String[] args) {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> broadcastSource = executionEnvironment.fromElements("a", "b", "c");

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("broadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<String>() {
        }));

        BroadcastStream<String> broadcast = broadcastSource.broadcast(mapStateDescriptor);


        executionEnvironment.fromElements(
                Tuple2.of("a", 1l),
                Tuple2.of("b", 2l),
                Tuple2.of("c", 3l),
                Tuple2.of("d", 2l),
                Tuple2.of("e", 2l),
                Tuple2.of("f", 2l)
        ).connect(broadcast)
         .process(new BroadcastProcessFunction<Tuple2<String, Long>, String, String>() {

             //处理非广播流中的数据
             @Override
             public void processElement(Tuple2<String, Long> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

             }

             //处理广播流中的数据元
             @Override
             public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {

             }
         });


    }


}
