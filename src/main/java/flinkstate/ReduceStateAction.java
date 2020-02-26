package flinkstate;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/26
 * @description： TODO
 */
public class ReduceStateAction {

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
         .flatMap(new ReduceStateFunction())
         .print();

        executionEnvironment.execute("reduceState");
    }

    public static class ReduceStateFunction extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String,Long>>{

        private transient ReducingState<Long> reducingState;

        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {

            reducingState.add(value.f1);

            out.collect(Tuple2.of(value.f0,reducingState.get()));

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ReducingStateDescriptor reducingStateDescriptor = new ReducingStateDescriptor<Long>(
                    "reduceState",
                    new ReduceFunction<Long>() {
                        @Override
                        public Long reduce(Long value1, Long value2) throws Exception {
                            return value1+value2;
                        }
                    },
                    TypeInformation.of(new TypeHint<Long>() {
                    })
            );

            reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
        }
    }
}
