package flinkstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * KeyState案例
 *
 * @author gavin
 * @createDate 2019/11/26
 *
 * interface State
 *      void clear();
 *
 *      interface ValueState<T>
 *          T value() throws IOException;
 *          void update(T value) throws IOException;
 *
 *      interface ListState<T>
 *          void add(T value) throws Exception;
 *          Iterable<T> get() throws Exception;
 *          void update(List<T> values) throws Exception;
 *          void addAll(List<T> values) throws Exception;
 *
 *       interface MapState<UK, UV>
 *           UV get(UK key) throws Exception;
 *           void put(UK key, UV value) throws Exception;
 *           void putAll(Map<UK, UV> map) throws Exception;
 *           void remove(UK key) throws Exception;
 *           boolean contains(UK key) throws Exception;
 *           Iterable<Map.Entry<UK, UV>> entries() throws Exception;
 *           Iterable<UK> keys() throws Exception;
 *           Iterable<UV> values() throws Exception;
 *           Iterator<Map.Entry<UK, UV>> iterator() throws Exception;
 *
 * abstract class StateDescriptor<S extends State, T> implements Serializable
 *      ValueStateDescriptor<T> extends StateDescriptor<ValueState<T>, T>
 *      ListStateDescriptor<T> extends StateDescriptor<ListState<T>, List<T>>
 *      MapStateDescriptor<UK, UV> extends StateDescriptor<MapState<UK, UV>, Map<UK, UV>>
 *      ReducingStateDescriptor<T> extends StateDescriptor<ReducingState<T>, T>
 *      AggregatingStateDescriptor<IN, ACC, OUT> extends StateDescriptor<AggregatingState<IN, OUT>, ACC>
 *
 *      protected final String name;
 *      private TypeInformation<T> typeInfo;
 *      private StateTtlConfig ttlConfig = StateTtlConfig.DISABLED;
 *
 * abstract class TypeInformation<T> implements Serializable
 *      public static <T> TypeInformation<T> of(TypeHint<T> typeHint) {
            return typeHint.getTypeInfo();
        }

  class StateTtlConfig implements Serializable
        状态过期参数设置
        private final UpdateType updateType;
        private final StateVisibility stateVisibility;
        private final TtlTimeCharacteristic ttlTimeCharacteristic;
        private final Time ttl;
        private final CleanupStrategies cleanupStrategies;
 *
 *
 *
 *
 */
public class TestKeyedState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

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

        DataStream<Tuple2<Long,Long>> dataStream=env.fromElements(
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

        dataStream.keyBy(0)
                .flatMap(new KeysMapState())
                .print();
        env.execute();
    }

    public static class KeysValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private transient ValueState<Tuple2<Long,Long>> valueState;

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {

            Tuple2<Long, Long> currentsum = Optional.ofNullable(valueState.value()).orElse(new Tuple2<>(0L, 0L));

            currentsum.f0+=1;
            currentsum.f1+=value.f1;

            valueState.update(currentsum);

            if (currentsum.f0 == 3){
                out.collect(new Tuple2<>(value.f0,currentsum.f1/currentsum.f0));
                valueState.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor(
                    "valueState",
                    TypeInformation.of(new TypeHint<Tuple2<Long,Long>>() {
                    })
            );

            valueState = getRuntimeContext().getState(valueStateDescriptor);
        }
    }

    public static class KeysListState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Iterable<Long>>>{

        private transient ListState<Long> listState;

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Iterable<Long>>> out) throws Exception {

            listState.add(value.f1);

            Iterable<Long> iterable = listState.get();

            out.collect(new Tuple2<>(value.f0,iterable));

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor listStateDescriptor = new ListStateDescriptor(
                    "listStateDes",
                    TypeInformation.of(new TypeHint<Long>() {
                    })
            );

            listState = getRuntimeContext().getListState(listStateDescriptor);
        }
    }

    public static class KeysMapState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,String>>{

        private transient MapState<Long,Long> mapState;

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, String>> out) throws Exception {

            Long count = Optional.ofNullable(mapState.get(value.f1)).orElse(1L);

            count+=1;

            mapState.put(value.f1,count);

            Iterator<Long> keys = mapState.keys().iterator();
            Iterator<Long> values = mapState.values().iterator();

            StringBuilder builder = new StringBuilder("");

            while(keys.hasNext() && values.hasNext()){
                Long key = keys.next();
                Long newValue = values.next();

                builder.append(key+":"+newValue+"|");
            }

            out.collect(Tuple2.of(value.f0,builder.toString()));
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            MapStateDescriptor mapStateDescriptor = new MapStateDescriptor(
                    "mapState",
                    TypeInformation.of(new TypeHint<Long>() {
                    }),
                    TypeInformation.of(new TypeHint<Long>() {
                    })
                  );

            mapState = getRuntimeContext().getMapState(mapStateDescriptor);


        }
    }


}
