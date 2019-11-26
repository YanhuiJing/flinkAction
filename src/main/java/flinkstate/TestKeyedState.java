package flinkstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * KeyState案例
 *
 * @author gavin
 * @createDate 2019/11/26
 */
public class TestKeyedState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long,Long>> inputStream=env.fromElements(
                Tuple2.of(1L,4L),
                Tuple2.of(2L,3L),
                Tuple2.of(3L,1L),
                Tuple2.of(1L,2L),
                Tuple2.of(3L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(2L,9L),
                Tuple2.of(2L,9L),
                Tuple2.of(3L,20L),
                Tuple2.of(2L,25L),
                Tuple2.of(2L,9L),
                Tuple2.of(2L,9L)
        );

        inputStream
                .keyBy(0)
                .flatMap(new CountWithKeyedState())
                .setParallelism(10)
                .print();
        env.execute();
    }
}

class CountWithKeyedState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {

    private transient ValueState<Tuple2<Long,Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {

        Tuple2<Long,Long> currentSum=sum.value();

        if(null==currentSum){
            currentSum=Tuple2.of(0L,0L);
        }


        currentSum.f0+=1;

        currentSum.f1+=value.f1;

        sum.update(currentSum);

        if(currentSum.f0>=3){
            out.collect(Tuple2.of(value.f0,currentSum.f1/currentSum.f0));
            sum.clear();
        }


    }

    @Override
    public void open(Configuration parameters) throws Exception {

        /**
         * 注意这里仅仅用了状态，但是没有利用状态来容错
         */
        ValueStateDescriptor<Tuple2<Long,Long>> descriptor=
                new ValueStateDescriptor<>(
                        "avgState",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})
                );
        sum=getRuntimeContext().getState(descriptor);


    }
}
