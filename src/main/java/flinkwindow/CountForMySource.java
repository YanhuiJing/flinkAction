package flinkwindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Random;

/**
 * 自定义source
 *
 * @author dajiangtai
 * @create 2019-06-09-16:36
 */
public class CountForMySource {

    public static void main(String[] args) throws Exception{
        //解析命令行参数
        ParameterTool params = ParameterTool.fromArgs(args);

        //获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        DataStreamSource<Long> dataStream = env.addSource(new SimpleSourceFunction());
//
//        DataStream<Long> numDataStream = dataStream.map(new MapFunction<Long, Long>() {
//            @Override
//            public Long map(Long value) throws Exception {
//                System.out.println("数据源="+value);
//                return value;
//            }
//        });
//
//
//
//       numDataStream.timeWindowAll(Time.seconds(4),Time.seconds(2))
//                .sum(0)
//                .print();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(3000);

        env.addSource(new SimpleTimeSource())
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<String, Long> element) {
                        return element.f1;
                    }
                })
                .keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.seconds(3)));
//                .process()
//                .process(new ProcessWindowFunction<Tuple2<String, Long>, Integer, Tuple, TimeWindow>() {
//
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Integer> out) throws Exception {
//                        Iterator<Tuple2<String, Long>> iterator = elements.iterator();
//                        int count =0;
//                        while(iterator.hasNext()){
//                            count+=1;
//                            iterator.next();
//                        }
//
//                        out.collect(count);
//                    }
//                }).print();
//                .timeWindow(Time.seconds(5), Time.seconds(3))
//                .process(new ProcessWindowFunction<Tuple2<String, Long>, Object, Tuple, TimeWindow>() {
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Object> out) throws Exception {
//                        Iterator<Tuple2<String, Long>> iterator = elements.iterator();
//                        int count =0;
//                        while(iterator.hasNext()){
//                            count+=1;
//                            iterator.next();
//                        }
//
//                        out.collect(count);
//                    }
//                }).print();

//                .reduce((ReduceFunction<Tuple3<String, Long, Integer>>) (value1, value2) -> new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2));




        env.execute("CountForMySource");


    }

    public  static  class SimpleSourceFunction implements ParallelSourceFunction<Long> {
        private long num = 0L;
        private volatile boolean isRunning = true;

        // 电脑有8core,默认有8个数据源
        @Override
        public void run(SourceContext<Long> sourceContext) throws Exception {
            while (isRunning){
                sourceContext.collect(num);
                num++;
                Thread.sleep(2000);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class SimpleTimeSource implements ParallelSourceFunction<Tuple2<String,Long>>{

        private volatile boolean isRunning = true;
        private int count =0;


        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {

            Random random = new Random();
            while(isRunning){

//                if(count % 5 ==0){
//                    ctx.collect(new Tuple2<>("A",System.currentTimeMillis()-new Random().nextInt(10)));
//                }else {
//                    ctx.collect(new Tuple2<>("A",System.currentTimeMillis()));
//                }
                long currentTime = System.currentTimeMillis();
                ctx.collect(new Tuple2<>("A",currentTime));

                int sleep = random.nextInt(5);

                System.out.println(currentTime+","+sleep);
                Thread.sleep(sleep);

            }

        }

        @Override
        public void cancel() {

            isRunning = false;

        }
    }

//    public static class SimpleSession extends RichSourceFunction<String> {
//
//        private volatile Boolean isRunning = true;
//
//        @Override
//        public void run(SourceContext<String> ctx) throws Exception {
//
//            while(isRunning){
//                ctx.collect("A");
//                Thread.sleep(new Random().nextInt(5));
//            }
//
//        }
//
//        @Override
//        public void cancel() {
//            isRunning = false;
//        }
//    }


}
