package flinkMetrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

/**
 *
 * @author gavin
 * @createDate 2020/3/1
 * https://www.cnblogs.com/zhaowei121/p/11906901.html
 * Counter可以用来计算数据累加
 */
public class CounterAction {

    public static class CounterFunction extends RichMapFunction<String,String> {

        private transient Counter counter;

        @Override
        public String map(String value) throws Exception {
            counter.inc();
            return value;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.counter = getRuntimeContext()
                    .getMetricGroup()
                    .counter("myCounter");

        }
    }
}
