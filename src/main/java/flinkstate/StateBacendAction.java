package flinkstate;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/27
 * @description： TODO
 */
public class StateBacendAction {

    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置堆内存存储,后端存储把数据以内部对象的形式保存在 Task Managers 的内存（JVM 堆）中，
        // 当应用程序触发 checkpoint 时，会将此时的状态进行快照然后存储在 Job Manager 的内存中。
        executionEnvironment.setStateBackend(new MemoryStateBackend());
        //设置文件存储,后端存储把数据以内部对象的形式保存在 Task Managers 的内存（JVM 堆）中，
        // 当应用程序触发 checkpoint 时，会将文件存储到指定的目录,常见的是hdfs目录
        executionEnvironment.setStateBackend(new FsStateBackend(""));
        //设置RockDB存储,RocksDB 是一种{嵌入式的本地数据库}，它会在本地文件系统中维护状态，
        //KeyedStateBackend 等会直接写入本地 RocksDB 中，它还需要配置一个文件系统（一般是 HDFS），
        //比如 hdfs://namenode:40010/flink/checkpoints，当触发 checkpoint 的时候，
        // 会把整个 RocksDB 数据库复制到配置的文件系统中去，当 failover 时从文件系统中将数据恢复到本地
        //RocksDB优点:
        //1,state 直接存放在 RocksDB 中，不需要存在内存中，这样就可以减少 Task Manager 的内存压力，
        //  如果是存内存的话大状态的情况下会导致 GC 次数比较多，同时还能在 checkpoint 时将状态持久化
        //  到远端的文件系统，那么就比较适合在生产环境中使用
        //2,RocksDB 本身支持 checkpoint 功能
        //3,RocksDBStateBackend 支持增量的 checkpoint，在 RocksDBStateBackend 中有一个字段
        //  enableIncrementalCheckpointing 来确认是否开启增量的 checkpoint，默认是不开启的，
        //  在 CheckpointingOptions 类中有个 state.backend.incremental 参数来表示，
        //  增量 checkpoint 非常使用于超大状态的场景。
        executionEnvironment.setStateBackend(new RocksDBStateBackend(""));

    }
}
