package flinkexample.userPurchaseBehavior.start;


import flinkexample.userPurchaseBehavior.function.ConnectedBroadcastProcessFunction;
import flinkexample.userPurchaseBehavior.model.Config;
import flinkexample.userPurchaseBehavior.model.EvaluatedResult;
import flinkexample.userPurchaseBehavior.model.UserEvent;
import flinkexample.userPurchaseBehavior.schema.ConfigDeserializationSchema;
import flinkexample.userPurchaseBehavior.schema.EvaluatedResultSerializationSchema;
import flinkexample.userPurchaseBehavior.schema.UserEventDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 主程序入口
 *
 * @author dajiangtai
 * @create 2019-06-24-9:05
 */
public class Launcher {
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String GROUP_ID ="group.id";
    public static final String INPUT_EVENT_TOPIC = "input-event-topic";
    public static final String INPUT_CONFIG_TOPIC = "input-config-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    public static final String  RETRIES = "retries";
    //keyed state 数据结构
    public static final MapStateDescriptor<String,Config> configStateDescriptor =
            new MapStateDescriptor<String, Config>("configBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Config>() {
            }));
    
    public static void main(String[] args) throws Exception{
        //获取执行环境
        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //检查输入参数
        ParameterTool params = parameterCheck(args);

//        设置time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * checkpoint
         */
        //启动checkp
        env.enableCheckpointing(600000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //语义保证
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        //checkpoint 超时时间
        checkpointConfig.setCheckpointTimeout(10000L);
        //启动外部持久化检查点
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * stateBackend
         */
        //env.setStateBackend(new FsStateBackend("hdfs://mycluster/flink-checkpoints/customer-purchase-behavior-tracker"));

        /**
         * restart 策略
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(30,TimeUnit.SECONDS)));

        /**
         * Kafka consumer
         */
        Properties consumerProps = new Properties();
        consumerProps.setProperty(BOOTSTRAP_SERVERS,params.get(BOOTSTRAP_SERVERS));
        consumerProps.setProperty(GROUP_ID,params.get(GROUP_ID));

        /**
         * 读取kafka事件流
         */
        final FlinkKafkaConsumer010<UserEvent> kafkaUserEventSource = new FlinkKafkaConsumer010<UserEvent>(params.get(INPUT_EVENT_TOPIC), new UserEventDeserializationSchema(), consumerProps);

        KeyedStream<UserEvent, String> customerUserEventStream = env.addSource(kafkaUserEventSource)
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(org.apache.flink.streaming.api.windowing.time.Time.hours(24)))
                .keyBy(new KeySelector<UserEvent, String>() {
                    @Override
                    public String getKey(UserEvent userEvent) throws Exception {
                        return userEvent.getUserId();
                    }
                });
        customerUserEventStream.print();

        /**
         * 读取Kafka 配置流信息
         */
        final FlinkKafkaConsumer010<Config> kafkaConfigEventSource = new FlinkKafkaConsumer010<Config>(params.get(INPUT_CONFIG_TOPIC), new ConfigDeserializationSchema(), consumerProps);
        final BroadcastStream<Config> configBroadcastStream = env.addSource(kafkaConfigEventSource)
                .broadcast(configStateDescriptor);

        /**
         * 连接事件流和配置流
         */
        DataStream<EvaluatedResult> connectedStream = customerUserEventStream
                                .connect(configBroadcastStream)
                                .process(new ConnectedBroadcastProcessFunction());

        Properties producerProps = new Properties();
        producerProps.setProperty(BOOTSTRAP_SERVERS,params.get(BOOTSTRAP_SERVERS));
        producerProps.setProperty(RETRIES,"3");

        final  FlinkKafkaProducer010<EvaluatedResult> kafkaProducer = new FlinkKafkaProducer010(params.get(OUTPUT_TOPIC), new EvaluatedResultSerializationSchema(), producerProps);

        /**
         * at_least_once 配置
         * https://www.jianshu.com/p/c25bde9893a7?from=groupmessage&isappinstalled=0
         */
        kafkaProducer.setLogFailuresOnly(false);
        kafkaProducer.setFlushOnCheckpoint(true);

        connectedStream.addSink(kafkaProducer);

        env.execute("UserPurchaseBehaviorTracker");
    }

    /**
     * 参数校验
     */
    public static ParameterTool parameterCheck(String[] args){
        ParameterTool params = ParameterTool.fromArgs(args);

        if(!params.has(BOOTSTRAP_SERVERS)){
            System.err.println("----------------parameter[bootstrap.servers] is required-------------------------");
            System.exit(-1);
        }

        if(!params.has(GROUP_ID)){
            System.err.println("----------------parameter[group.id] is required-------------------------");
            System.exit(-1);
        }

        if(!params.has(INPUT_EVENT_TOPIC)){
            System.err.println("----------------parameter[input-event-topic] is required-------------------------");
            System.exit(-1);
        }

        if(!params.has(INPUT_CONFIG_TOPIC)){
            System.err.println("----------------parameter[input-config-topic] is required-------------------------");
            System.exit(-1);
        }

        if(!params.has(OUTPUT_TOPIC)){
            System.err.println("----------------parameter[output-topic] is required-------------------------");
            System.exit(-1);
        }

        return params;
    }

    /**
     * 自定义watermark
     */
    private static class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent>{
        public CustomWatermarkExtractor(org.apache.flink.streaming.api.windowing.time.Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserEvent element) {
            return element.getEventTime();
        }
    }
}
