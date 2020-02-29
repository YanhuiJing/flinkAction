package flinkwindow;

import flinkconnector.Tuple2Source;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author： JingYanHui@11111717
 * @date： 2020/2/27
 * @description：
 * Window 组件之 WindowAssigner
 *      到达窗口操作符的元素被传递给WindowAssigner,WindowsAssigner将元素分配给一个或多个窗口,可能创建新的窗口
 *      ***窗口本身只是元素列表的标识符***,它可能提供一些可选的元信息,例如TimeWindow中的开始和结束时间。注意,元素
 *      可以被添加到多个窗口,这也意味着一个元素可以在多个窗口存在
 *
 *      GlobalWindows                    => 所有数据都可以分配到同一个窗口
 *      TumblingProcessingTimeWindows    => 基于处理时间的滚动窗口分配处理
 *      TumblingEventTimeWindows         => 基于时间时间的滚动窗口分配处理
 *      SlidingProcessingTimeWindows     => 基于处理时间的滑动窗口分配处理
 *      SlidingEventTimeWindows          => 基于事件时间的滑动窗口分配处理
 *      ProcessingTimeSessionWindows     => 基于处理时间的会话窗口分配处理
 *      EventTimeSessionWindows          => 基于事件时间的会话窗口分配处理
 *
 * Window 组件之 Trigger
 *      Trigger表示触发器,每个窗口都拥有一个Trigger(触发器),该Trigger决定何时计算和清除窗口,当先前计算的计时器超时
 *      时,将为插入窗口的每个元素调用触发器。每个事件上,触发器都可以决定触发,即清除(删除窗口并丢弃内容),或者启动并清除
 *      窗口。一个窗口可以被求值多次,在清除之前一直存在。在清除窗口之前,窗口将一直消耗内存。
 *
 *      public enum TriggerResult {
 *
 *          //不做任何操作
 *          CONTINUE(false, false),
 *
 *          //处理并移除窗口中的数据
 *          FIRE_AND_PURGE(true, true),
 *
 *          //处理窗口数据，窗口计算后不做清理
 *          FIRE(true, false),
 *
 *          //清除窗口中的所有元素，并且在不计算窗口函数或不发出任何元素的情况下丢弃窗口
 *          PURGE(false, true);
 *
 *       }
 *
 *      EventTimeTrigger                 => 当水印通过窗口末尾时触发的触发器
 *      ProcessingTimeTrigger            => 当系统时间通过窗口末尾时触发的触发器
 *      DeltaTrigger                     => 一种基于DeltaFunction和阈值触发的触发器
 *      CountTrigger                     => 一旦窗口中的元素数量达到给定数量时就触发的触发器
 *      PurgingTrigger                   => 一种触发器,可以将任何触发器转换为清除触发器
 *      ContinuousProcessingTimeTrigger  => 触发器根据给定的时间间隔连续触发,时间间隔依赖于job所在机器系统时间
 *      ContinuousEventTimeTrigger       => 触发器根据给定的时间间隔连续触发,时间间隔依赖于水印时间戳
 *      NeverTrigger                     => 一个从来不触发的触发器,作为GlobalWindow的默认触发器
 *
 * Window 组件之 Evictor
 *      Evictor表示驱逐者,它可以遍历窗口元素列表,并可以决定从列表的开头删除首先进入窗口的一些元素,然后将其余的元素
 *      被赋给一个计算函数,如果没有定义Evictor,触发器直接将所有窗口元素交给计算函数
 *
 *      TimeEvictor                      => 元素可以在窗口中存在一段时间,老数据被清除
 *      CountEvictor                     => 窗口中可以保存指定数量的数据,超过则会清除老数据
 *      DeltaEvictor                     => 根据DeltaFunction的实现和阈值来决定如何清理数据
 *
 */
public class TimeWindowAction {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //默认是processingTime
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        executionEnvironment.addSource(new Tuple2Source(60))
                            .keyBy(0)
                            .timeWindow(Time.seconds(9),Time.seconds(3))
                            .sum(1)
                            .print();

        executionEnvironment.execute("timeWindowTest");


    }


}


