package flinkconnector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.WordCount;

import java.util.Arrays;
import java.util.Random;

/**
 * flink自定义Source
 *
 * @author gavin
 * @createDate 2019/11/25
 *
 *      自定义数据源接口
 *      interface SourceFunction
 *          void run(SourceContext<T> ctx) throws Exception;
 *          void cancel();
 *          interface SourceContext<T>
 *              void collect(T element);
 *              void collectWithTimestamp(T element, long timestamp);
 *              void emitWatermark(Watermark mark);
 *              void markAsTemporarilyIdle();
 *              Object getCheckpointLock();
 *              void close();
 */
public class DefineSource {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String,Integer>> dataStream = env.addSource(new EventTimeSource())
                .flatMap(new FlatMapFunction<Tuple2<String,Long>, Tuple3<String,Long,Integer>>() {
                    @Override
                    public void flatMap(Tuple2<String, Long> value, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String[] splits = value.f0.toLowerCase().split("\\W+");
                        Long currentTime = value.f1;
                        Arrays.asList(splits).stream().filter((elem) -> (elem.length()>0))
                                .forEach((elem) -> {
                                    out.collect(new Tuple3<>(elem,currentTime,1));
                                });
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element) {
                        return element.f1;
                    }
                })
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .sum(2)
                .map(new MapFunction<Tuple3<String,Long,Integer>, Tuple2<String,Integer>>() {

                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, Long, Integer> value) throws Exception {
                        return Tuple2.of(value.f0,value.f2);
                    }
                });

        dataStream.print();

        env.execute("EventTimeSource");

    }

    public static class LineSource implements SourceFunction<String>{

        private int lineLength = WordCount.WORDS.length;
        private volatile Boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(isRunning){
                ctx.collect(WordCount.WORDS[new Random().nextInt(lineLength)]);
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /*
        自定义数据源可以通过下面方法自己设定时间戳和水印,不过不建议这么做,在数据流处理逻辑中进行指定更加清晰
        ctx.collectWithTimestamp(value,value.f1);
        ctx.emitWatermark(new Watermark(currentTime-1000L));
     */
    public static class EventTimeSource implements SourceFunction<Tuple2<String,Long>>{

        private int lineLength = WordCount.WORDS.length;
        private Boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {

            while(isRunning){
                long currentTime = System.currentTimeMillis();
                ctx.collect(new Tuple2<>(WordCount.WORDS[new Random().nextInt(lineLength)],currentTime));
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
