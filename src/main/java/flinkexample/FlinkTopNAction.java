package flinkexample;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.Words;

import java.util.*;

/**
 * @author gavin
 * @createDate 2020/3/10
 */
public class FlinkTopNAction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

                 executionEnvironment.addSource(new SourceFunction<String>() {

                     private volatile boolean isRunning = true;

                     private int size = Words.WORDS.length-1;

                     private Random random = new Random();

                    @Override
                    public void run(SourceContext ctx) throws Exception {

                        while(true){

                            ctx.collect(Words.WORDS[random.nextInt(size)]);
                            Thread.sleep(500);

                        }

                    }

                    @Override
                    public void cancel() {
                        isRunning = false;
                    }
                }).flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.split("\\W+");

                        for (String split : splits) {
                            if (split.length() > 0) {
                                out.collect(Tuple2.of(split, 1));
                            }

                        }
                    }
                }).keyBy(0)
                .timeWindow(Time.seconds(30),Time.seconds(10))
                .sum(1)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new TopNAllFunction(10))
                .print();

                executionEnvironment.execute("topN");


    }

    private static class TopNAllFunction
            extends
            ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> {

        private int topSize = 10;

        public TopNAllFunction(int topSize) {

            this.topSize = topSize;
        }

        @Override
        public void process(
                ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>.Context arg0,
                Iterable<Tuple2<String, Integer>> input,
                Collector<Tuple2<String, Integer>> out) throws Exception {
            PriorityQueue<Tuple2<String,Integer>> queue = new PriorityQueue<>(topSize, new Comparator<Tuple2<String, Integer>>() {
                @Override
                public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                    return o1.f1 - o2.f1;
                }
            });

            for(Tuple2<String,Integer> tuple:input){
                if(queue.size() < topSize){
                    queue.offer(tuple);
                }else{
                    Tuple2<String, Integer> peek = queue.peek();
                    if(tuple.f1 > peek.f1){
                        queue.poll();
                        queue.offer(tuple);
                    }
                }
            }

            while (!queue.isEmpty()){
                out.collect(queue.poll());
            }

            //使用treeMap实现topN
//            TreeMap<Integer, Tuple2<String, Integer>> treemap = new TreeMap<Integer, Tuple2<String, Integer>>(
//                    new Comparator<Integer>() {
//
//                        @Override
//                        public int compare(Integer y, Integer x) {
//                            // TODO Auto-generated method stub
//                            return (x < y) ? -1 : 1;
//                        }
//
//                    }); //treemap按照key降序排列，相同count值不覆盖
//
//            for (Tuple2<String, Integer> element : input) {
//                treemap.put(element.f1, element);
//                if (treemap.size() > topSize) { //只保留前面TopN个元素
//                    treemap.pollLastEntry();
//                }
//            }
//
//            for (Map.Entry<Integer, Tuple2<String, Integer>> entry : treemap
//                    .entrySet()) {
//                out.collect(entry.getValue());
//            }

        }

    }

}
