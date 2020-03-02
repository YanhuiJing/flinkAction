package flinkstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
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
 *    interface ListState<T>
 *        void add(T value) throws Exception;
 *        Iterable<T> get() throws Exception;
 *        void update(List<T> values) throws Exception;
 *        void addAll(List<T> values) throws Exception;
 */
public class ListStateAction {

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
                .flatMap(new ListStateFunction())
                .print();

        executionEnvironment.execute("listState");

    }

    public static class ListStateFunction extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String, Iterable<Long>>>{

        private transient ListState<Long> listState;

        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Iterable<Long>>> out) throws Exception {

            listState.add(value.f1);

            Iterable<Long> iterable = listState.get();

            out.collect(Tuple2.of(value.f0,iterable));

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor listStateDescriptor = new ListStateDescriptor(
                    "listState",
                    TypeInformation.of(new TypeHint<Long>(){})
            );

            listState = getRuntimeContext().getListState(listStateDescriptor);
        }

    }



}

