package flinkwindow;

import flinkconnector.Tuple2Source;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/27
 * @description： TODO
 */
public class SessionWindowAction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 使用sessionWindow时,如果数据间的间隔大于指定会话窗口才会触发窗口执行,如果一致小于窗口则窗口不会执行
        executionEnvironment.addSource(new Tuple2Source(100))
                            .keyBy(0)
                            .window(ProcessingTimeSessionWindows.withGap(Time.seconds(4)))
                            .sum(1)
                            .print();

        executionEnvironment.execute("sessionWindow");

        }
}
