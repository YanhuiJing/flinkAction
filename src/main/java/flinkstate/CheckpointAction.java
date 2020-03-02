package flinkstate;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/27
 * @description： TODO
 *
 * Flink 的 Checkpoint 有以下先决条件：
 * 1,需要具有持久性且支持重放一定时间范围内数据的数据源。例如：Kafka、RabbitMQ 等。这里为什么要求支持重放一定时间范围内的数据呢？
 * 因为 Flink 的容错机制决定了，当 Flink 任务失败后会自动从最近一次成功的 Checkpoint 处恢复任务，此时可能需要把任务失败前消费
 * 的部分数据再消费一遍，所以必须要求数据源支持重放。假如一个Flink 任务消费 Kafka 并将数据写入到 MySQL 中，任务从 Kafka 读取到数据，
 * 还未将数据输出到 MySQL 时任务突然失败了，此时如果 Kafka 不支持重放，就会造成这部分数据永远丢失了。支持重放数据的数据源可以保障任务
 * 消费失败后，能够重新消费来保障任务不丢数据。
 * 2,需要一个能保存状态的持久化存储介质，例如：HDFS、S3 等。当 Flink 任务失败后，自动从 Checkpoint 处恢复，但是如果 Checkpoint
 * 时保存的状态信息快照全丢了，那就会影响 Flink 任务的正常恢复。就好比我们看书时经常使用书签来记录当前看到的页码，当下次看书时找到书签
 * 的位置继续阅读即可，但是如果书签三天两头经常丢，那我们就无法通过书签来恢复阅读。
 */
public class CheckpointAction {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // flink包含fixed-delay（固定延时重启策略）、failure-rate（故障率重启策略）、none（不重启策略）三种重启策略
        // FixedDelayRestartStrategy 是固定延迟重启策略，程序按照集群配置文件中或者程序中额外设置的重启次数尝试重启作业，
        // 如果尝试次数超过了给定的最大次数，程序还没有起来，则停止作业，另外还可以配置连续两次重启之间的等待时间
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
        //      3, // 尝试重启的次数
        //      Time.of(10, TimeUnit.SECONDS) // 延时
        //  ));

        //  FailureRateRestartStrategy 是故障率重启策略，在发生故障之后重启作业，如果固定时间间隔之内发生故障的次数超过设置的值后，
        //  作业就会失败停止，该重启策略也支持设置连续两次重启之间的等待时间。
        //  env.setRestartStrategy(RestartStrategies.failureRateRestart(
        //      3, // 固定时间间隔允许 Job 重启的最大次数
        //      Time.of(5, TimeUnit.MINUTES), // 固定时间间隔
        //      Time.of(10, TimeUnit.SECONDS) // 两次重启的延迟时间
        //  ));


        // 开启 Checkpoint，每 1000毫秒进行一次 Checkpoint
        env.enableCheckpointing(1000);

        // Checkpoint 语义设置为 EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // CheckPoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 同一时间，只允许 有 1 个 Checkpoint 在发生
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 两次 Checkpoint 之间的最小时间间隔为 500 毫秒
        // 两次 Checkpoint 之间的最小时间间隔：从上一次 Checkpoint 结束到下一次 Checkpoint 开始，中间的间隔时间。
        // 例如，env.enableCheckpointing(60000) 表示 1 分钟触发一次 Checkpoint，同时再设置两次 Checkpoint
        // 之间的最小时间间隔为 30 秒，假如任务运行过程中一次 Checkpoint 就用了50s，那么等 Checkpoint 结束后，
        // 理论来讲再过 10s 就要开始下一次 Checkpoint 了，但是由于设置了最小时间间隔为30s，所以需要再过 30s 后，
        // 下次 Checkpoint 才开始。注：如果配置了该参数就决定了同时进行的 Checkpoint 数量只能为 1。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // 当 Flink 任务取消时，保留外部保存的 CheckPoint 信息
        // ExternalizedCheckpointCleanup.RETAINONCANCELLATION：当作业手动取消时，保留作业的 Checkpoint 状态信息。
        // 注意，这种情况下，需要手动清除该作业保留的 Checkpoint 状态信息，否则这些状态信息将永远保留在外部的持久化存储中。
        // ExternalizedCheckpointCleanup.DELETEONCANCELLATION：当作业取消时，Checkpoint 状态信息会被删除。
        // 仅当作业失败时，作业的 Checkpoint 才会被保留用于任务恢复。
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 当有较新的 Savepoint 时，作业也会从 Checkpoint 处恢复
//        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // 作业最多允许 Checkpoint 失败 1 次（flink 1.9 开始支持）
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);

        // Checkpoint 失败后，整个 Flink 任务也会失败（flink 1.9 之前）
//        env.getCheckpointConfig().setFailTasksOnCheckpointingErrors(true)

        // 在程序中给算子分配 Operator ID，以便来升级程序。主要通过 uid(String) 方法手动指定算子的 ID ，
        // 这些 ID 将用于恢复每个算子的状态。程序恢复的时可以依据指定算子的id恢复程序逻辑
        env.fromElements("a","b","c")
                // Stateful source (e.g. Kafka) with ID
                .uid("source-id") // ID for the source operator
                .shuffle()
                // Stateful mapper with ID
                .map(String::toUpperCase)
                .uid("mapper-id") // ID for the mapper
                // Stateless printing sink
                .print(); // Auto-generated ID

        //如果不为算子手动指定 ID，Flink 会为算子自动生成 ID。当 Flink 任务从 Savepoint 中恢复时，是按照
        //Operator ID 将快照信息与算子进行匹配的，只要这些 ID 不变，Flink 任务就可以从 Savepoint 中恢复。
        // 自动生成的 ID 取决于代码的结构，并且对代码更改比较敏感，因此强烈建议给程序中所有有状态的算子手动分配
        // Operator ID。如下左图所示，一个 Flink 任务包含了 算子 A 和 算子 B，代码中都未指定 Operator ID，
        // 所以 Flink 为 Task A 自动生成了 Operator ID 为 aaa，为 Task B 自动生成了 Operator ID 为 bbb，
        // 且 Savepoint 成功完成。但是在代码改动后，任务并不能从 Savepoint 中正常恢复，因为 Flink 为算子生成的
        // Operator ID 取决于代码结构，代码改动后可能会把算子 B 的 Operator ID 改变成 ccc，导致任务从 Savepoint 恢复时，
        // SavePoint 中只有 Operator ID 为 aaa 和 bbb 的状态信息，算子 B 找不到 Operator ID 为 ccc 的状态信息，
        // 所以算子 B 不能正常恢复。

        // Savepoint 需要用户手动去触发，触发 Savepoint 的方式如下所示：
        // bin/flink savepoint :jobId [:targetDirectory]
        // 这将触发 ID 为 :jobId 的作业进行 Savepoint，并返回创建的 Savepoint 路径，用户需要此路径来还原和删除 Savepoint 。
        // 使用 YARN 触发 Savepoint 的方式如下所示：
        // bin/flink savepoint :jobId [:targetDirectory] -yid :yarnAppId
        // 这将触发 ID 为 :jobId 和 YARN 应用程序 ID :yarnAppId 的作业进行 Savepoint，并返回创建的 Savepoint 路径。
        // 使用 Savepoint 取消 Flink 任务：
        // bin/flink cancel -s [:targetDirectory] :jobId
        // 这将自动触发 ID 为 :jobid 的作业进行 Savepoint，并在 Checkpoint 结束后取消该任务。此外，可以指定一个目标文件系统
        // 目录来存储 Savepoint 的状态信息，也可以在 flink 的 conf 目录下 flink-conf.yaml 中配置 state.savepoints.dir
        // 参数来指定 Savepoint 的默认目录，触发 Savepoint 时，如果不指定目录则使用该默认目录。无论使用哪种方式配置，都需要保障
        // 配置的目录能被所有的 JobManager 和 TaskManager 访问。



    }

}
