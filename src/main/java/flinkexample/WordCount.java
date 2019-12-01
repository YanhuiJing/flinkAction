package flinkexample;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * wordCount主类
 *
 * @author gavin
 * @createDate 2019/11/25
 *  StreamGraph是对用户逻辑的映射,JobGraph在次基础上进行了一些优化,把一部分操作串成chain以提高效率。
 *  ExecutionGraph是为了调度存在的,加入了并行度的概念。而在此基础上真正执行task
 *  abstract class StreamExecutionEnvironment
 *      String DEFAULT_JOB_NAME = "Flink Streaming Job"
 *      TimeCharacteristic DEFAULT_TIME_CHARACTERISTIC = TimeCharacteristic.ProcessingTime;\
 *      long DEFAULT_NETWORK_BUFFER_TIMEOUT = 100L
 *      // Jobc参数配置
 *      ExecutionConfig config = new ExecutionConfig()
 *      // Checkpoint配置
 *      CheckpointConfig checkpointCfg = new CheckpointConfig()
 *      // job执行链条,job解析过程中算子逐个加入到列表中,execute触发任务执行时,遍历每个transformation生成StreamGraph
 *      List<Transformation<?>> transformations = new ArrayList<>()
 *      // 外部缓存配置
 *      StateBackend defaultStateBackend
 *      List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cacheFile = new ArrayList<>()
 *
 *      //通过execute()触发任务执行
 *      public JobExecutionResult execute(String jobName) throws Exception {
 *               Preconditions.checkNotNull(jobName, "Streaming Job name should not be null.");
 *
 *               return execute(getStreamGraph(jobName));
 *           }
*      	public StreamGraph getStreamGraph(String jobName) {
 *               return getStreamGraphGenerator().setJobName(jobName).generate();
 *          }
 * abstract class Transformation<T>
 *     // 每一个transformation都有一个唯一标识的id
 *     Integer idCounter = 0;
 *     // transformation输出数据类型
 *     TypeInformation<T> outputType;
 *     // 唯一标识id,名称,输出类型,并行度,共享槽组
 *     public Transformation(String name, TypeInformation<T> outputType, int parallelism) {
 *           this.id = getNewNodeId();
 *           this.name = Preconditions.checkNotNull(name);
 *           this.outputType = outputType;
 *           this.parallelism = parallelism;
 *           this.slotSharingGroup = null;
 *           }
 *  // datatStram主要由StreamExecutionEnvironment和Transformation组成,每个算子处理之后会生成新的dataStream
 * class DataStream<T>
        protected final StreamExecutionEnvironment environment;
        protected final Transformation<T> transformation;
        public DataStream(StreamExecutionEnvironment environment, Transformation<T> transformation) {
            this.environment = Preconditions.checkNotNull(environment, "Execution Environment must not be null.");
            this.transformation = Preconditions.checkNotNull(transformation, "Stream Transformation must not be null.");
        }
 class StreamingJobGraphGenerator
    // 判断两个StreamNode是否能chain在一起
    public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
        StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

        StreamOperatorFactory<?> headOperator = upStreamVertex.getOperatorFactory();
        StreamOperatorFactory<?> outOperator = downStreamVertex.getOperatorFactory();

        下游Edges的长度为1 ||  上下游操作不为空  ||  上下游算子的共享槽相同  || 下游的chain策略是always而上游的策略是always或者head
        edge的shuffle模式不是BATCH模式  ||  上下游的并行度数量相等  ||  streamGraph是可连接的
        return downStreamVertex.getInEdges().size() == 1
            && outOperator != null
            && headOperator != null
            && upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
            && outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
            && (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
            headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
            && (edge.getPartitioner() instanceof ForwardPartitioner)
            && edge.getShuffleMode() != ShuffleMode.BATCH
            && upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
            && streamGraph.isChainingEnabled();
    }

 *
 *
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));



        env.fromElements(WORDS)
                // 如果使用lambda表达式,每次执行完之后都需要通过TypeInformation进行格式转换,逻辑显得比较复杂
//                .flatMap((value,out)->{
//                    String[] splits = value.toLowerCase().split("\\W+");
//
//                        Arrays.asList(splits).stream().filter((elem) -> (elem.length()>0))
//                                .forEach((elem) -> {
//                                    out.collect(new Tuple2<>(elem, 1));
//                                });
//
//                })
//                .returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.toLowerCase().split("\\W+");

                        Arrays.asList(splits).stream().filter((elem) -> (elem.length()>0))
                                .forEach((elem) -> {
                                    out.collect(new Tuple2<>(elem, 1));
                                });
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value1.f1);
                    }
                })
                .print();
        //Streaming 程序必须加这个才能启动程序，否则不会有结果
        env.execute("gavin —— word count streaming demo");
    }

    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer"
    };
}
