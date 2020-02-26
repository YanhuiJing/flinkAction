package flinkwindow;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

/**
 * 测试AggFunction
 *
 * @author dajiangtai
 * @create 2019-06-11-18:10
 */
public class TestAggFunctionOnWindow {
    public static void main(String[] args) throws Exception{
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStream<Tuple3<String,String,Long>> input = env.fromElements(ENGLISH);

        //求各个班级英语成绩平均分
        DataStream<Double> avgScore = input.keyBy(0)
                .countWindow(2)
                .aggregate(new AverageAggrate());

        avgScore.print();

        env.execute("TestAggFunctionOnWindow");


    }

    public static final Tuple3[] ENGLISH = new Tuple3[]{
            Tuple3.of("class1","张三",100L),
            Tuple3.of("class1","李四",78L),
            Tuple3.of("class1","王五",99L),
            Tuple3.of("class2","赵六",81L),
            Tuple3.of("class2","小七",59L),
            Tuple3.of("class2","小八",97L),
    };

    public static class AverageAggrate implements AggregateFunction<Tuple3<String, String, Long>, Tuple2<Long, Long>, Double>{
        /**
         * 创建累加器保存中间状态（sum  count）
         * @return
         */
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L,0L);
        }


        /**
         * 将元素添加到累加器并返回新的累加器,单个类加起计算

         * @return
         */
        @Override
        public Tuple2<Long, Long> add(Tuple3<String, String, Long> value, Tuple2<Long, Long> acc) {
            return new Tuple2<>(acc.f0+value.f2,acc.f1+1L);
        }

        /**
         * 从累加器提取结果
         * @return
         */
        @Override
        public Double getResult(Tuple2<Long, Long> acc) {
            return ((double)acc.f0)/acc.f1;
        }

        /**
         * 累加器合并
         * @param acc1
         * @return
         */
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
            return new Tuple2<>(acc1.f0+acc2.f0,acc1.f1+acc2.f1);
        }
    }

}
