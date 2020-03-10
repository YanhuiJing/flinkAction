package flinkstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Optional;


/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/26
 * @description：
 * interface ValueState<T>
 *     T value() throws IOException;
 *     void update(T value) throws IOException;
 */
public class ValueStateAction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.fromElements(
                Tuple2.of("a",1L),
                Tuple2.of("a",2L),
                Tuple2.of("a",3L),
                Tuple2.of("b",2L),
                Tuple2.of("b",2L),
                Tuple2.of("b",2L)
        ).keyBy(0)
         .flatMap(new ValueStateFunction())
         .print();

        executionEnvironment.execute("valueState");


    }


    public static class ValueStateFunction extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String,Long>>{

        private transient ValueState<Long> valueState;

        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {

            Long oldValue = Optional.ofNullable(valueState.value()).orElse(0L);

            long newValue = value.f1 + oldValue;

            valueState.update(newValue);

            out.collect(Tuple2.of(value.f0,newValue));
        }

        /**
         * 定义状态描述
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {


            ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor(
                    "valueState",
                    TypeInformation.of(new TypeHint<Long>() {
                    })
            );

            valueState = getRuntimeContext().getState(valueStateDescriptor);

        }
    }

}
