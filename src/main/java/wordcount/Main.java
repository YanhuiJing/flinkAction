package wordcount;

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
 */
public class Main {

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
