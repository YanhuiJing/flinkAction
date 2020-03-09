package flinkexample;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.Words;

/**
 * SideOutputExample
 *
 * @author gavin
 * @createDate 2019/11/26
 */
public class SideOutputAction {

    /**
     * We need to create an {@link OutputTag} so that we can reference it when emitting
     * data to a side output and also to retrieve the side output stream from an operation.
     */
    private static final OutputTag<String> rejected3Tag = new OutputTag<String>("rejected-3") {};
    private static final OutputTag<String> rejected5Tag = new OutputTag<String>("rejected-5") {};
    private static final OutputTag<String> rejected7Tag = new OutputTag<String>("rejected-7") {};

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = env.fromElements(Words.WORDS)
                .keyBy(new KeySelector<String, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer getKey(String value) throws Exception {
                        return 0;
                    }
                })
                .process(new Tokenizer());

        DataStream<String> rejected3Words = tokenized
                .getSideOutput(rejected3Tag)
                .map(new MapFunction<String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String map(String value) throws Exception {
                        return "3: " + value;
                    }
                });

        SingleOutputStreamOperator<String> rejected5Words = tokenized.getSideOutput(rejected5Tag)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "5: " + value;
                    }
                });

        SingleOutputStreamOperator<String> rejected7Words = tokenized.getSideOutput(rejected7Tag)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "7: " + value;
                    }
                });

        rejected3Words.print();
        rejected5Words.print();
        rejected7Words.print();


        // execute program
        env.execute("Streaming Words SideOutput");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     *
     * <p>This rejects words that are longer than 5 characters long.
     */
    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(
                String value,
                Context ctx,
                Collector<Tuple2<String, Integer>> out) throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() >= 3 && token.length()<5) {
                    ctx.output(rejected3Tag, token);
                } else if(token.length()>=5 && token.length()<7){
                    ctx.output(rejected5Tag,token);
                }else if(token.length()>7){
                    ctx.output(rejected7Tag,token);
                } else if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }

        }
    }
}

