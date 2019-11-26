package flinkconnector.kafkaconnector;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka生产者工具类
 *
 * @author gavin
 * @createDate 2019/11/26
 *
 * 进程守护模式启动kafka
 * nohup bin/kafka-server-start.sh config/server.properties >/dev/null 2>&1 &
 */
public class KafkaProduceUtils {

    public static final String broker_list = "192.168.57.101:9092,192.168.57.102:9092,192.168.57.103:9092";
    public static final String topic = "metric";  // kafka topic，Flink 程序中需要和这个统一

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Metric metric = new Metric();
        metric.setTimeStamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();

        tags.put("cluster", "zhisheng");
        tags.put("host_ip", "101.147.022.106");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, JSON.toJSONString(metric));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(3000);
            writeToKafka();
        }
    }
}
