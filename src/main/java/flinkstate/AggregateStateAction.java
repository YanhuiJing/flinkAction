package flinkstate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/26
 * @description：
 * AggregatingState: 保存很多值的聚合结果的单一值，与 ReducingState 相比，
 *     不同点在于聚合类型可以和元素类型不同，提供 AggregateFunction 来实现聚合。
 */
public class AggregateStateAction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.fromElements(
                Tuple2.of("a",1l),
                Tuple2.of("a",2l),
                Tuple2.of("a",3l),
                Tuple2.of("b",2l),
                Tuple2.of("b",2l),
                Tuple2.of("b",2l)
        ).keyBy(0)
                .flatMap(new ReduceStateAction.ReduceStateFunction())
                .print();

        executionEnvironment.execute("aggregateState");

    }

    public static class AggregateStateFunction extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String,Long>>{
        //结合业务的复杂程度,在一个算子中可以包含多个状态
        private transient AggregatingState<Long,Long> aggregatingState;

        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {

            aggregatingState.add(value.f1);
            out.collect(Tuple2.of(value.f0,aggregatingState.get()));

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            AggregatingStateDescriptor aggregatingStateDescriptor = new AggregatingStateDescriptor(
                    "aggregatingState",
                    new AggregateFunction<Long,Long,Long>() {

                        @Override
                        public Long createAccumulator() {
                            return 0l;
                        }

                        @Override
                        public Long add(Long value, Long accumulator) {
                            return value+accumulator;
                        }

                        @Override
                        public Long getResult(Long accumulator) {
                            return accumulator;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return a+b;
                        }
                    }, TypeInformation.of(new TypeHint<Long>() {
            }));

            aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);


        }

    }

}


