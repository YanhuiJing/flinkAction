package flinkconnector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 自定义测试数据源
 *
 * @author gavin
 * @createDate 2020/2/29
 */
public class Tuple2Source implements SourceFunction<Tuple2<String,Integer>> {

    private volatile boolean isRunning = true;

    private List<Tuple2<String,Integer>> tupleList = Arrays.asList(
            Tuple2.of("a",1),
            Tuple2.of("b",2),
            Tuple2.of("c",3),
            Tuple2.of("d",4),
            Tuple2.of("e",5),
            Tuple2.of("f",6)
    );

    private int count;

    public Tuple2Source(int count){

        this.count = count;

    }

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {

        while(isRunning){

            for(int i=0;i<count;i++){
                for(Tuple2<String,Integer> tuple:tupleList){

                    Thread.sleep(1000);
                    ctx.collect(tuple);


                }
            }

        }

    }

    @Override
    public void cancel() {

        isRunning = false;

    }


}
