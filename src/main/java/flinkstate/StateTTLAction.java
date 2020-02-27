package flinkstate;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/27
 * @description：
 * 1、newBuilder 方法的第一个参数是必需的，它代表着状态存活时间。
 *
 * 2、UpdateType 配置状态 TTL 更新时（默认为 OnCreateAndWrite）：
 *
 * StateTtlConfig.UpdateType.OnCreateAndWrite: 仅限创建和写入访问时更新
 *
 * StateTtlConfig.UpdateType.OnReadAndWrite: 除了创建和写入访问，还支持在读取时更新
 *
 * 3、StateVisibility 配置是否在读取访问时返回过期值（如果尚未清除），默认是 NeverReturnExpired：
 *
 * StateTtlConfig.StateVisibility.NeverReturnExpired: 永远不会返回过期值
 *
 * StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp: 如果仍然可用则返回
 *
 * 在 NeverReturnExpired 的情况下，过期状态表现得好像它不再存在，即使它仍然必须被删除。该选项对于在 TTL 之后必须严格用于读取访问的数据的用例是有用的，例如，应用程序使用隐私敏感数据.
 *
 * 另一个选项 ReturnExpiredIfNotCleanedUp 允许在清理之前返回过期状态。
 *
 * 注意：
 *
 * 状态后端会存储上次修改的时间戳以及对应的值，这意味着启用此功能会增加状态存储的消耗，堆状态后端存储一个额外的 Java 对象，其中包含对用户状态对象的引用和内存中原始的 long 值。RocksDB 状态后端存储为每个存储值、List、Map 都添加 8 个字节。
 *
 * 目前仅支持参考 processing time 的 TTL
 *
 * 使用启用 TTL 的描述符去尝试恢复先前未使用 TTL 配置的状态可能会导致兼容性失败或者 StateMigrationException 异常。
 *
 * TTL 配置并不是 Checkpoint 和 Savepoint 的一部分，而是 Flink 如何在当前运行的 Job 中处理它的方式。
 *
 * 只有当用户值序列化器可以处理 null 值时，具体 TTL 的 Map 状态当前才支持 null 值，如果序列化器不支持 null 值，则可以使用 NullableSerializer 来包装它（代价是需要一个额外的字节）。
 */
public class StateTTLAction extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String,Long>> {

    private transient ValueState<Long> valueState;

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig stateTtlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor("valueState",Long.class);

        valueStateDescriptor.enableTimeToLive(stateTtlConfig);

        valueState = getRuntimeContext().getState(valueStateDescriptor);





    }
}


