package flinkconnector.kafkaconnector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * flink消费kafka数据
 *
 * @author gavin
 * @createDate 2019/11/26
 * FlinkKafkaConsumer
 * 不同版本的 FlinkKafkaConsumer 都继承自 FlinkKafkaConsumerBase 抽象类，所以可知 FlinkKafkaConsumerBase 是最核心的类了。
 * FlinkKafkaConsumerBase 实现了 CheckpointedFunction、CheckpointListener 接口，继承了 RichParallelSourceFunction
 * 抽象类来读取 Kafka 数据
 *
 * 在 FlinkKafkaConsumerBase 中的 open 方法中做了大量的配置初始化工作，然后在 run 方法里面是由 AbstractFetcher 来获取数据的，
 * 在 AbstractFetcher 中有用 List> 来存储着所有订阅分区的状态信息，包括了下面这些字段：
 *
 *      private final KafkaTopicPartition partition;    //分区
 *      private final KPH kafkaPartitionHandle;
 *      private volatile long offset;   //消费到的 offset
 *      private volatile long committedOffset;
 *
 * FlinkKafkaProducer
 * FlinkKafkaProducer 这个有些特殊，不同版本的类结构有些不一样，如 FlinkKafkaProducer011 是继承的 TwoPhaseCommitSinkFunction
 * 抽象类，而 FlinkKafkaProducer010 和 FlinkKafkaProducer09 是基于 FlinkKafkaProducerBase 类来实现的。
 * 在 Kafka 0.11.x 版本后支持了事务，这让 Flink 与 Kafka 的事务相结合从而实现端到端的 Exactly once 才有了可能
 *
 * 想要获取数据的元数据信息
 * 在消费 Kafka 数据的时候，有时候想获取到数据是从哪个 Topic、哪个分区里面过来的，这条数据的 offset 值是多少。这些元数据信息在有的场景真的需要，
 * 在获取数据进行反序列化的时候使用 KafkaDeserializationSchema 就行。
 * 在 KafkaDeserializationSchema 接口中的 deserialize 方法里面的 ConsumerRecord 类中是包含了数据的元数据信息。
 * public class ConsumerRecord<K, V> {
 *     private final String topic;
 *     private final int partition;
 *     private final long offset;
 *     private final long timestamp;
 *     private final TimestampType timestampType;
 *     private final long checksum;
 *     private final int serializedKeySize;
 *     private final int serializedValueSize;
 *     private final K key;
 *     private final V value;
 * }
 *
 * Kafka数据序列化
 *      读取kakfa反序列化 => KafkaDeserializationSchema 或者 DeserializationSchema
 *      写入kafka序列化 => SerializationSchema
 *
 */
public class kafkasource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        // kafka消费参数设置
        props.put("bootstrap.servers", "192.168.57.101:9092,192.168.57.102:9092,192.168.57.103:9092");
        props.put("zookeeper.connect", "192.168.57.101:2181,192.168.57.102:2181,192.168.57.103:2181");
        props.put("group.id", "metric-group");

        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 =
                new FlinkKafkaConsumer011<>("metric", new SimpleStringSchema(), props);

        flinkKafkaConsumer011.setStartFromLatest();

        FlinkKafkaProducer011<String> flinkKafkaProducer011 =
                new FlinkKafkaProducer011<>("192.168.57.101:9092,192.168.57.102:9092,192.168.57.103:9092",
                                            "metric", new SimpleStringSchema());

        //https://www.jianshu.com/p/c25bde9893a7?from=groupmessage&isappinstalled=0
        flinkKafkaProducer011.setLogFailuresOnly(false);

        env.addSource(flinkKafkaConsumer011)
            .addSink(flinkKafkaProducer011);


        env.execute("Flink add data source");
    }
}
