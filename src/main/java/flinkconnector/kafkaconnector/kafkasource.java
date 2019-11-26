package flinkconnector.kafkaconnector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * flink消费kafka数据
 *
 * @author gavin
 * @createDate 2019/11/26
 */
public class kafkasource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        // kafka消费参数设置
        props.put("bootstrap.servers", "192.168.57.101:9092,192.168.57.102:9092,192.168.57.103:9092");
        props.put("zookeeper.connect", "192.168.57.101:2181,192.168.57.102:2181,192.168.57.103:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                "metric",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props)).setParallelism(1);

        dataStreamSource.print(); //把从 kafka 读取到的数据打印在控制台

        env.execute("Flink add data source");
    }
}
