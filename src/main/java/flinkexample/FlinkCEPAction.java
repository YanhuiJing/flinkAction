package flinkexample;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author gavin
 * @createDate 2020/3/1
 * https://www.cnblogs.com/zhaowei121/p/12060736.html
 * https://blog.csdn.net/qq_31866793/article/details/102834803
 * https://cloud.tencent.com/developer/article/1448608
 * https://www.sohu.com/a/207384581_609376
 *
 */
public class FlinkCEPAction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<Event, String> eventDataStream = executionEnvironment.addSource(new SourceFunction<Tuple3<String, Integer, Integer>>() {

            private volatile boolean isRunning = true;
            private List<Tuple2<String, Integer>> list = Arrays.asList(
                        Tuple2.of("a", 42),
                        Tuple2.of("a", 20),
                        Tuple2.of("b", 42),
                        Tuple2.of("b", 30),
                        Tuple2.of("c", 30),
                        Tuple2.of("c", 10)
                    );

            @Override
            public void run(SourceContext<Tuple3<String, Integer, Integer>> ctx) throws Exception {
                int count = 0;
                while (isRunning) {
                    Thread.sleep(1000);
                    Tuple2<String, Integer> tuple = list.get(count % 6);
                    ctx.collect(Tuple3.of(tuple.f0, tuple.f1, count));
                    ++count;
                }

            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>, Event>() {
            @Override
            public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Event> out) throws Exception {
                out.collect(new Event(value.f0, value.f1, value.f2));
            }
        }).keyBy(data -> data.getName());

        // 定义复杂事件处理逻辑,将满足过滤规则条件的数据过滤出来
        Pattern<Event, ?> pattern = Pattern.<Event>begin("start", AfterMatchSkipStrategy.noSkip()).where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getId() == 42;
                    }
                }
        ).followedByAny("middle").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getId() >15;
                    }
                }
        ).within(Time.seconds(3));


        CEP.pattern(eventDataStream, pattern).select(new PatternSelectFunction<Event,String>(){

            @Override
            public String select(Map<String, List<Event>> pattern) throws Exception {

                return pattern.toString();
            }
        }).print();

        executionEnvironment.execute("flinkCEP");

    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Event {
    private String name;
    private Integer id;
    private Integer index;

}
