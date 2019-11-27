package flinkexample.userPurchaseBehavior.function;


import flinkexample.userPurchaseBehavior.model.*;
import flinkexample.userPurchaseBehavior.start.Launcher;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.base.*;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 处理函数
 *
 * @author dajiangtai
 * @create 2019-06-24-14:15
 */
public class ConnectedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, UserEvent, Config, EvaluatedResult> {
    private Config defaultConfig = new Config("APP","2018-01-01",0,3);
    private final MapStateDescriptor<String, Map<String, UserEventContainer>> userMapStateDesc =
            new MapStateDescriptor<String, Map<String, UserEventContainer>>("userEventContainerState", BasicTypeInfo.STRING_TYPE_INFO,new MapTypeInfo<String, UserEventContainer>(String.class,UserEventContainer.class));
    @Override
    public void processElement(UserEvent value, ReadOnlyContext ctx, Collector<EvaluatedResult> out) throws Exception {

        //获取用户id
        String userId = value.getUserId();
        //获取渠道
        String channel = value.getChannel();

        //获取事件类型
        EventType eventType = EventType.valueOf(value.getEventType());

        //根据渠道channel 获取广播变量
        Config config = ctx.getBroadcastState(Launcher.configStateDescriptor).get(channel);

        //判断广播变量的值是否为空
        if(Objects.isNull(config)){
            config = defaultConfig;
        }

        //channel,uid:UserEventContainer
        final  MapState<String, Map<String, UserEventContainer>> state = getRuntimeContext().getMapState(userMapStateDesc);

        //uid:UserEventContainer
        Map<String, UserEventContainer> userEventContainerMap = state.get(channel);
        //判断userEventContainerMap是否为空
        if(Objects.isNull(userEventContainerMap)){
            userEventContainerMap = Maps.newHashMap();
            state.put(channel,userEventContainerMap);
        }


        //判断userId是否为空
        if(!userEventContainerMap.containsKey(userId)){
            UserEventContainer container = new UserEventContainer();
            container.setUserId(userId);
            userEventContainerMap.put(userId,container);
        }

        //map 容器添加UserEvent
        userEventContainerMap.get(userId).getUserEvents().add(value);

        if(eventType == EventType.PURCHASE){
            //计算用户购买路径
            Optional<EvaluatedResult> result = compute(config, userEventContainerMap.get(userId));
            result.ifPresent(r -> out.collect(result.get()));

            state.get(channel).remove(userId);
        }

    }

    @Override
    public void processBroadcastElement(Config value, Context ctx, Collector<EvaluatedResult> out) throws Exception {

        //获取渠道
        String channel = value.getChannel();

        BroadcastState<String, Config> broadcastState = ctx.getBroadcastState(Launcher.configStateDescriptor);

        //更新config value for key
        broadcastState.put(channel,value);
    }

    /**
     * 计算购买路径长度
     */
    private Optional<EvaluatedResult> compute(Config config ,UserEventContainer container){

        //避免出现空指针
        Optional<EvaluatedResult> result = Optional.empty();

        //获取渠道
        String channel = config.getChannel();

        //历史购买次数
        Integer historyPurchaseTimes = config.getHistoryPurchaseTimes();

        //最大购买路径
        Integer maxPurchasePathLength = config.getMaxPurchasePathLength();
        
        //当前购买路径
        int purchasePathLen = container.getUserEvents().size();
        if(purchasePathLen>maxPurchasePathLength && historyPurchaseTimes < 10){
            //事件根据时间排序
            container.getUserEvents().sort(Comparator.comparingLong(UserEvent::getEventTime));

            //定义一个map集合
            final  Map<String,Integer> stat = Maps.newHashMap();

            container.getUserEvents()
                    .stream()
                    .collect(Collectors.groupingBy(UserEvent::getEventType))
                    .forEach((eventType,events ) -> stat.put(eventType,events.size()));

            final EvaluatedResult evaluatedResult = new EvaluatedResult();
            evaluatedResult.setUserId(container.getUserId());
            evaluatedResult.setChannel(channel);
            evaluatedResult.setEventTypeCounts(stat);
            evaluatedResult.setPurchasePathLength(purchasePathLen);

            result = Optional.of(evaluatedResult);

        }

        return result;
    }


}
