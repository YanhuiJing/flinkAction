package flinkexample.userPurchaseBehavior.schema;

import com.alibaba.fastjson.JSON;
import flinkexample.userPurchaseBehavior.model.EvaluatedResult;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * 计算结果序列化类
 *
 * @author dajiangtai
 * @create 2019-06-24-13:31
 */
public class EvaluatedResultSerializationSchema implements SerializationSchema<EvaluatedResult> {

    @Override
    public byte[] serialize(EvaluatedResult element) {
        return JSON.toJSONString(element).getBytes();
    }
}
