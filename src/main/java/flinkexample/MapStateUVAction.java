package flinkexample;

import flinkconnector.Tuple2Source;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Objects;

/**
 * 使用mapState进行uv统计
 *
 * @author gavin
 * @createDate 2020/3/2
 */
public class MapStateUVAction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig
                .Builder().setHost("192.168.30.244").build();

        executionEnvironment.addSource(new Tuple2Source(10))
                .keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String,Long>>() {

                    private MapState<String,Long> mapState;
                    private ValueState<Long> valueState;

                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Integer> value) throws Exception {
                        if(Objects.nonNull(valueState.value())){
                            valueState.update(0L);
                        }

                        if(!mapState.contains(value.f1.toString())){
                            mapState.put(value.f1.toString(),null);
                            valueState.update(valueState.value()+1);
                        }

                        String key = "";

                        return Tuple2.of(key,valueState.value());
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>(
                                "mapState",
                                 TypeInformation.of(new TypeHint<String>() {}),
                                 TypeInformation.of(new TypeHint<Long>() {})
                        ));

                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                                "valueState",
                                TypeInformation.of(new TypeHint<Long>() {
                                })
                        ));

                    }
                }).addSink(new RedisSink<>(conf,new RedisSetSinkMapper()));

        executionEnvironment.execute("mapState");
    }

    // 数据与 Redis key 的映射关系，并指定将数据 set 到 Redis
    public static class RedisSetSinkMapper
            implements RedisMapper<Tuple2<String, Long>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 这里必须是 set 操作，通过 MapState 来维护用户集合，
            // 输出到 Redis 仅仅是为了展示结果供其他系统查询统计结果
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(Tuple2<String, Long> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Long> data) {
            return data.f1.toString();
        }
    }
}
