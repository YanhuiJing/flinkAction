package flinkexample.userPurchaseBehavior.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * 配置流
 *
 * @author dajiangtai
 * @create 2019-06-24-13:06
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Config implements Serializable {
    private static final long serialVersionUID = 2175805295049245714L;
    private String channel;
    private String registerDate;
    private Integer historyPurchaseTimes;
    private Integer maxPurchasePathLength;


}
