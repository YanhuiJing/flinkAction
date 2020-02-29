package flinkwindow;


import flinkconnector.Tuple2Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/27
 * @description： TODO
 */
public class CountWindowAction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.addSource(new Tuple2Source(60))
                            .keyBy(0)
                            .countWindow(3,1)
                            .sum(1)
                            .print();

        executionEnvironment.execute("countWindow");


    }


}



