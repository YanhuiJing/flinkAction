package flinkMetrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;

/**
 *
 * @author gavin
 * @createDate 2020/3/1
 *
 * Gauge 是最简单的 Metrics，它反映当前时刻的某个值
 * 比如要看现在 Java heap 内存用了多少，就可以每次实时的暴露一个 Gauge，Gauge 当前的值就是heap使用的量
 */
public class GaugeAction {


    public static class GaugeFunction extends RichMapFunction<String,String>{

        private transient int valueToExpose = 0;

        @Override
        public String map(String value) throws Exception {
            valueToExpose++;
            return value;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext()
                    .getMetricGroup()
                    .gauge("my_gauge", new Gauge<Integer>() {
                        @Override
                        public Integer getValue() {
                            return valueToExpose;
                        }
                    });
        }
    }


}
