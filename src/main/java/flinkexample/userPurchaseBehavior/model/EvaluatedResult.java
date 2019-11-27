package flinkexample.userPurchaseBehavior.model;

import lombok.Data;
import lombok.ToString;

import java.util.Map;

/**
 * 计算结果
 *
 * @author dajiangtai
 * @create 2019-06-24-13:28
 */
@Data
public class EvaluatedResult {
    private String userId;
    private String channel;
    private Integer purchasePathLength;
    private Map<String,Integer> eventTypeCounts;
}
