package flinkconnector.kafkaconnector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 统计数据基类
 *
 * @author gavin
 * @createDate 2019/11/26
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Metric {

    private String name;
    private long timeStamp;
    private Map<String, Object> fields;
    private Map<String, String> tags;


}
