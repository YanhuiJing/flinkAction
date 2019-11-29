package flinkfunction;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * flink方法集
 *
 * @author gavin
 * @createDate 2019/11/26
 *
 * interface function extends java.io.Serializable
 *      interface MapFunction<T, O> extends Function
 *          O map(T value) throws Exception;
 *      interface FlatMapFunction<T, O> extends Function
 *          void flatMap(T value, Collector<O> out) throws Exception;
 *      interface FilterFunction<T> extends Function
 *          boolean filter(T value) throws Exception;
 *      interface AggregateFunction<IN, ACC, OUT> extends Function
 *          ACC createAccumulator();
 *          ACC add(IN value, ACC accumulator);
 *          OUT getResult(ACC accumulator);
 *          ACC merge(ACC a, ACC b);
 *      interface RichFunction extends Function
 *          // 初始化,可以在方法调用之前,初始化获取状态机,广播变量,累加器等
 *          void open(Configuration parameters) throws Exception;
 *          void close() throws Exception;
 *          RuntimeContext getRuntimeContext();
 *          IterationRuntimeContext getIterationRuntimeContext();
 *          void setRuntimeContext(RuntimeContext t);
 *
 * 继承RichFunction,通过RuntimeContext获取任务配置,数据状态,累加器数据等
 * interface RuntimeContext
 *      String getTaskName();
 *      MetricGroup getMetricGroup();
 *      int getNumberOfParallelSubtasks();
 *      int getMaxNumberOfParallelSubtasks();
 *      int getIndexOfThisSubtask();
 *      int getAttemptNumber();
 *      ExecutionConfig getExecutionConfig();
 *      ClassLoader getUserCodeClassLoader();
 *      <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator);
 *      <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name);
 *      DistributedCache getDistributedCache();
 *      <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties);
 *      <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties);
 *      <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties);
 *      <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties);
 *      <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties);
 *      <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties);
 *
 *
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

    // 数据流分流
    public static class OutPutExample implements OutputSelector<String>{

        @Override
        public Iterable<String> select(String value) {

            return value.equals("gavin") ? Arrays.asList("gavin") : Arrays.asList("other");

        }
    }

    public static class AverageAccumulator {
        public long count;
        public long sum;
    }


}


