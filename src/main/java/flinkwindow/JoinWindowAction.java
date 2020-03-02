package flinkwindow;

import flinkconnector.Tuple2Source;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * windowjoin样例
 *
 * @author gavin
 * @createDate 2020/3/2
 */
public class JoinWindowAction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataSource01 = executionEnvironment.addSource(new Tuple2Source(60));
        DataStreamSource<Tuple2<String, Integer>> dataSource02 = executionEnvironment.addSource(new Tuple2Source(60));

        dataSource01.join(dataSource02)
                .where(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(10L),Time.seconds(3L)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        return first.f0+","+first.f1+","+second.f1;
                    }
                }).print();

        executionEnvironment.execute("windowJoin");

    }
}
