package flinkconnector.kafkaconnector;

import flinkconnector.DefineSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.common.metrics.Metrics;

/**
 * flink输出数据到kafka
 *
 * @author gavin
 * @createDate 2019/11/26
 */
public class kafkaSink {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new DefineSource.LineSource())
                .addSink(new FlinkKafkaProducer011<String>(
                        "192.168.57.101:9092,192.168.57.102:9092,192.168.57.103:9092",
                        "metric",
                        new SimpleStringSchema()
                ));

        env.execute();
    }
}
