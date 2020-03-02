package flinkstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Optional;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/26
 * @description： TODO
 *        interface MapState<UK, UV>
 *             UV get(UK key) throws Exception;
 *             void put(UK key, UV value) throws Exception;
 *             void putAll(Map<UK, UV> map) throws Exception;
 *            void remove(UK key) throws Exception;
 *            boolean contains(UK key) throws Exception;
 *            Iterable<Map.Entry<UK, UV>> entries() throws Exception;
 *            Iterable<UK> keys() throws Exception;
 *            Iterable<UV> values() throws Exception;
 *            Iterator<Map.Entry<UK, UV>> iterator() throws Exception;
 */

public class MapStateAction {

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
         .flatMap(new MapStateFunction())
         .print();

        executionEnvironment.execute("mapState");
    }

    public static class MapStateFunction extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String,String>>{

        private transient MapState<Long,Long> mapState;

        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, String>> out) throws Exception {

            Long num = Optional.ofNullable(mapState.get(value.f1)).orElse(0l);
            mapState.put(value.f1,num+1l);

            Iterable<Map.Entry<Long, Long>> entries = mapState.entries();

            out.collect(Tuple2.of(value.f0,entries.toString()));

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor mapStateDescriptor = new MapStateDescriptor(
                    "mapState",
                    TypeInformation.of(new TypeHint<Long>(){}),
                    TypeInformation.of(new TypeHint<Long>(){})
            );

            mapState = getRuntimeContext().getMapState(mapStateDescriptor);

        }
    }

}


