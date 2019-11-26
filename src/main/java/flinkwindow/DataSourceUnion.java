package flinkwindow;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 多数据源连接
 *
 * @author dajiangtai
 * @create 2019-06-09-16:54
 */
public class DataSourceUnion {

    public static void main(String[] args) throws  Exception{
        //解析命令行参数
        ParameterTool params = ParameterTool.fromArgs(args);

        //获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //接受数据
        DataStream<String> dataStream1 = env.fromElements("flink flink flink");

        DataStream<String> dataStream2 = env.fromElements("Spark Spark Spark");


        //数据处理，统计每个单词词频
        DataStream<Tuple2<String,Integer>> counts = dataStream1.union(dataStream2).flatMap(new DataSourceUnion.Tokenizer())
                .keyBy(0)
                .sum(1);

        //输出统计结果
        if(params.has("output")){
            counts.writeAsText(params.get("output"));
        }else{
            counts.print();
        }




        //执行flink程序
        env.execute("Streaming WordCount");
    }

    public  static final class  Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for(String token:tokens){
                out.collect(new Tuple2<>(token,1));
            }
        }
    }
}
