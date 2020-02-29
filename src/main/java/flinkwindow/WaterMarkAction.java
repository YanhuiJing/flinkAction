package flinkwindow;

import flinkconnector.Tuple2Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 *
 * @author gavin
 * @createDate 2020/2/29
 *
 * Watermark是一种衡量EventTime进展的机制,它是数据本身的一个隐藏属性,数据本身
 * 携带着对应的 Watermark。Watermark 本质来说就是一个时间戳，代表着比这时间戳
 * 早的事件已经全部到达窗口，即假设不会再有比这时间戳还小的事件到达，这个假设是触
 * 发窗口计算的基础，只有 Watermark 大于窗口对应的结束时间，窗口才会关闭和进行
 * 计算。按照这个标准去处理数据，那么如果后面还有比这时间戳更小的数据，那么就视为
 * 迟到的数据，对于这部分迟到的数据，Flink 也有相应的机制（下文会讲）去处理
 *
 * Watermark的两种方式:
 * AssignerWithPunctuatedWatermarks
 *        数据流中每一个递增的EventTime都会产生一个Watermark,在实际生产环境中,
 * 在TPS很高的情况下,会产生大量的Watermark,可能在一定程度上对下游算子造成一定的压力
 * 所以在实时性要求很高的情况下才会选择这种方式进行水印生成
 *        AssignerWithPunctuatedWatermarks 接口中包含了 checkAndGetNextWatermark
 * 方法，这个方法会在每次 extractTimestamp() 方法被调用后调用，它可以决定是否要生成一个新的
 * 水印，返回的水印只有在不为 null 并且时间戳要大于先前返回的水印时间戳的时候才会发送出去，如果
 * 返回的水印是 null 或者返回的水印时间戳比之前的小则不会生成新的水印。
 *
 *
 * public class WordPunctuatedWatermark implements AssignerWithPunctuatedWatermarks<Word> {
 *
 *     @Nullable
 *     @Override
 *     public Watermark checkAndGetNextWatermark(Word lastElement, long extractedTimestamp) {
 *         return extractedTimestamp % 3 == 0 ? new Watermark(extractedTimestamp) : null;
 *     }
 *
 *     @Override
 *     public long extractTimestamp(Word element, long previousElementTimestamp) {
 *         return element.getTimestamp();
 *     }
 * }
 *
 * 需要注意的是这种情况下可以为***每个事件都生成一个水印***，但是因为水印是要在下游参与计算的，所以过多的话会导致整体计算性能下降。
 *
 * AssignerWithPeriodicWatermarks：
 *      周期性的（一定时间间隔或者达到一定的记录条数）产生一个 Watermark。
 *      在实际的生产环境中，通常这种使用较多，它会周期性产生 Watermark 的方式，但是必须结合时间或者积累条数两个维度，
 * 否则在极端情况下会有很大的延时
 *
 * public class WordWatermark implements AssignerWithPeriodicWatermarks<Word> {
 *
 *     private long currentTimestamp = Long.MIN_VALUE;
 *
 *     @Override
 *     public long extractTimestamp(Word word, long previousElementTimestamp) {
 *         if (word.getTimestamp() > currentTimestamp) {
 *             this.currentTimestamp = word.getTimestamp();
 *         }
 *         return currentTimestamp;
 *     }
 *
 *     @Nullable
 *     @Override
 *     public Watermark getCurrentWatermark() {
 *         long maxTimeLag = 5000;
 *         return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
 *
 *     }
 * }
 *
 * AssignerWithPeriodicWatermarks有四个实现类
 *      BoundedOutOfOrdernessTimestampExtractor：
 *      该类用来发出滞后于数据时间的水印，它的目的其实就是和我们上面定义的那个类作用是类似的，
 * 你可以传入一个时间代表着可以允许数据延迟到来的时间是多长
 *      CustomWatermarkExtractor：
 *      这是一个自定义的周期性生成水印的类，在这个类里面的数据是 KafkaEvent
 *      AscendingTimestampExtractor：
 *      时间戳分配器和水印生成器，用于时间戳单调递增的数据流，如果数据流的时间戳不是单调递增，那么会有专门的处理方法
 *      IngestionTimeExtractor：
 *      依赖于机器系统时间，它在 extractTimestamp 和 getCurrentWatermark 方法中是根据 System.currentTimeMillis()
 * 来获取时间的，而不是根据事件的时间，如果这个时间分配器是在数据源进 Flink 后分配的，那么这个时间就和 Ingestion Time 一致了，
 * 所以命名也取的就是叫 IngestionTimeExtractor。
 *
 *
 */
public class WaterMarkAction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //定义OutputTag,通过旁路获取延迟数据
        OutputTag<Tuple2<String,Integer>> lateDataTag = new OutputTag("late") {};

        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = executionEnvironment.addSource(new Tuple2Source(100))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Integer>>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> element) {
                        return element.f1.longValue();
                    }
                }).keyBy(0)
                .timeWindow(Time.seconds(60), Time.seconds(20))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(lateDataTag)
                .sum(1);

        outputStreamOperator.print();

        outputStreamOperator.getSideOutput(lateDataTag)
                .print();

        executionEnvironment.execute("watermark");


    }
}
