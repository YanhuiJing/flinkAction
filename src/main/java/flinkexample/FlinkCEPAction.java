package flinkexample;

import flinkconnector.Tuple2Source;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author gavin
 * @createDate 2020/3/1
 */
public class FlinkCEPAction {

    public static void main(String[] args) throws Exception {

        Logger log = LoggerFactory.getLogger(FlinkCEPAction.class);

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> eventDataStream = executionEnvironment.fromCollection(Arrays.asList(
                Tuple2.of("gavin", 42),
                Tuple2.of("gavin", 15),
                Tuple2.of("gavin", 42),
                Tuple2.of("gavin", 20),
                Tuple2.of("gavin", 42),
                Tuple2.of("gavin", 10)
        )).flatMap(new FlatMapFunction<Tuple2<String, Integer>, Event>() {
            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Event> out) throws Exception {
                out.collect(new Event(value.f1, value.f0));
            }
        });

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        log.info("start {}", event.getId());
                        return event.getId() == 42;
                    }
                }
        ).next("middle").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        log.info("middle {}", event.getId());
                        return event.getId() >= 15;
                    }
                }
        );

        CEP.pattern(eventDataStream, pattern).flatSelect(new PatternFlatSelectFunction<Event, String>() {

            @Override
            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                for (Map.Entry<String, List<Event>> entry : map.entrySet()) {
                    collector.collect(entry.getKey() + " " + entry.getValue().get(0).getId() + "," + entry.getValue().get(0).getName());
                }
            }
        }).print();

        executionEnvironment.execute("flinkCEP");

    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Event {
    private Integer id;
    private String name;
}
