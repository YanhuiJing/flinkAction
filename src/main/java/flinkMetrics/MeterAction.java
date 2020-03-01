package flinkMetrics;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

/**
 *
 * @author gavin
 * @createDate 2020/3/1
 * Meter 是指统计吞吐量和单位时间内发生“事件”的次数。它相当于求一种速率，即事件次数除以使用的时间
 *
 */
public class MeterAction{

    public static class MeterFunction extends RichMapFunction<String,String>{

        private transient Meter meter;
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

            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("myMeter", new MeterView(counter, 20));
        }
    }

}
