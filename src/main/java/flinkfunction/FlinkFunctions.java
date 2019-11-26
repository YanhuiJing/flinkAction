package flinkfunction;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * flink方法集
 *
 * @author gavin
 * @createDate 2019/11/26
 */
public class FlinkFunctions {

    public static void main(String[] args) {

    }

    public static class MapExample implements MapFunction<String, Integer> {

        @Override
        public Integer map(String value) throws Exception {
            return value.length();
        }

    }

    public static class FlatMapExample implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value, 1));
        }
    }

    public static class FilterExample implements FilterFunction<String> {

        @Override
        public boolean filter(String value) throws Exception {
            return value.equals("gavin");
        }
    }

    public static class KeyByExample implements KeySelector<String, Integer> {
        //自定义KeyBy选项
        @Override
        public Integer getKey(String value) throws Exception {
            return value.length();
        }
    }

    public static class ReduceExample implements ReduceFunction<String> {

        @Override
        public String reduce(String value1, String value2) throws Exception {
            return value1.hashCode() > value2.hashCode() ? value1 : value2;
        }
    }

    // 数据聚合计算
    public static class AggregateExample implements AggregateFunction<Integer, AverageAccumulator, Double> {

        @Override
        public AverageAccumulator createAccumulator() {
            return new AverageAccumulator();
        }

        @Override
        public AverageAccumulator add(Integer value, AverageAccumulator accumulator) {
            accumulator.count += 1;
            accumulator.sum += value;

            return accumulator;
        }

        @Override
        public Double getResult(AverageAccumulator accumulator) {
            return Double.valueOf(accumulator.sum/accumulator.count);
        }

        @Override
        public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
            a.count += b.count;
            b.sum += b.sum;
            return a;
        }
    }


}


class AverageAccumulator {
    public long count;
    public long sum;
}
