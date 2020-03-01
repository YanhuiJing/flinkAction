package flinkconnector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Redis连接example
 *
 * @author gavin
 * @createDate 2020/3/1
 */
public class RedisConnectorAction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //单机redis配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        //redis集群配置
        FlinkJedisClusterConfig config = new FlinkJedisClusterConfig.Builder()
                .setNodes(new HashSet<InetSocketAddress>(
                        Arrays.asList(new InetSocketAddress("redis1", 6379)))).build();
        //Redis Sentinels 配置：
        FlinkJedisSentinelConfig sentinelConfig = new FlinkJedisSentinelConfig.Builder()
                .setMasterName("master")
                .setSentinels(new HashSet<>(Arrays.asList("sentinel1", "sentinel2")))
                .setPassword("")
                .setDatabase(1).build();

        executionEnvironment.fromCollection(Arrays.asList(
                Tuple2.of("a", "a"),
                Tuple2.of("b", "b"),
                Tuple2.of("c", "c"),
                Tuple2.of("d", "d")
        )).addSink(new RedisSink<>(conf, new RedisSinkMapper()));

        executionEnvironment.execute("redisSink");


    }

    /**
     *
     * 设置redis操作类型
     * public enum RedisCommand {
     *     LPUSH(RedisDataType.LIST),
     *     RPUSH(RedisDataType.LIST),
     *     SADD(RedisDataType.SET),
     *     SET(RedisDataType.STRING),
     *     PFADD(RedisDataType.HYPER_LOG_LOG),
     *     PUBLISH(RedisDataType.PUBSUB),
     *     ZADD(RedisDataType.SORTED_SET),
     *     HSET(RedisDataType.HASH);
     * }
     *
     */
    public static class RedisSinkMapper implements RedisMapper<Tuple2<String, String>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            //指定 RedisCommand 的类型是 HSET，对应 Redis 中的数据结构是 HASH，另外设置 key = zhisheng
            return new RedisCommandDescription(RedisCommand.HSET, "zhisheng");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
