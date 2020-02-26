package flinkconnector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import utils.Words;

import java.util.Random;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/26
 * @description： TODO
 */
public class WordSource {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        executionEnvironment.addSource(new LinesSource())
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                        String[] elements = value.split("\\W+");
                        for(String element:elements){
                            out.collect(Tuple2.of(element,1));
                        }
                    }
                }).keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                }).print();

        executionEnvironment.execute("sourceTest");


    }

    public static class LinesSource implements SourceFunction<String>{

        private int lineLength = Words.WORDS.length;
        private volatile Boolean isRunning = true;

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (isRunning){

                int randomLineNum = new Random().nextInt(lineLength);
                sourceContext.collect(Words.WORDS[randomLineNum]);
                Thread.sleep(1000);

            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
