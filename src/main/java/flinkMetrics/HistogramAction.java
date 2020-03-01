package flinkMetrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

/**
 *
 * @author gavin
 * @createDate 2020/3/1
 * Histogram 用于统计一些数据的分布，比如说 Quantile、Mean、StdDev、Max、Min 等。
 */
public class HistogramAction {

    public static class HistogramFunction extends RichMapFunction<String,String>{

        private transient Histogram histogram;

        @Override
        public void open(Configuration config) {
            this.histogram = getRuntimeContext()
                    .getMetricGroup()
                    .histogram("my_histogram", new DescriptiveStatisticsHistogram(10));
        }

        @Override
        public String map(String value) throws Exception {
            this.histogram.update(1);
            return value;
        }

    }


}
