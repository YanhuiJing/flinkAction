package flinkwindow;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试reduceFuntion
 *
 * @author dajiangtai
 * @create 2019-06-11-17:45
 */
public class TestReduceFunctionOnWindow {
    public static void main(String[] args) throws Exception{
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStream<Tuple3<String,String,Integer>> input = env.fromElements(ENGLISH);

        DataStream<Tuple3<String,String,Integer>>  totalPoints = input.keyBy(0).countWindow(2).reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
                return new Tuple3<>(value1.f0,value1.f1,value1.f2+value2.f2);
            }
        });

        totalPoints.print();

        env.execute("TestReduceFunctionOnWindow");

    }


    public static final Tuple3[] ENGLISH = new Tuple3[]{
            Tuple3.of("class1","张三",100),
            Tuple3.of("class1","李四",78),
            Tuple3.of("class1","王五",99),
            Tuple3.of("class2","赵六",81),
            Tuple3.of("class2","小七",59),
            Tuple3.of("class2","小八",97),
    };
}
