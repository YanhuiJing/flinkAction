package flinkexample.userPurchaseBehavior.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import flinkexample.userPurchaseBehavior.model.UserEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;


import java.io.IOException;

/**
 * 反序列化
 *
 * @author dajiangtai
 * @create 2019-06-24-10:18
 *
 * KeyedDeserializationSchema接口已经被废除,可以直接使用DeserializationSchema接口
 */
public class UserEventDeserializationSchema implements DeserializationSchema<UserEvent> {
    @Override
    public UserEvent deserialize(byte[] message) throws IOException {
        return JSON.parseObject(new String(message),new TypeReference<UserEvent>(){});
    }

    @Override
    public boolean isEndOfStream(UserEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(new TypeHint<UserEvent>() {});
    }


}
