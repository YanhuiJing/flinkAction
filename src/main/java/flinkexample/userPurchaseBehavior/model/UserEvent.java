package flinkexample.userPurchaseBehavior.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * 用户事件
 *
 * @author dajiangtai
 * @create 2019-06-24-10:13
 */
@Data
public class UserEvent implements Serializable {

    private static final long serialVersionUID = 7325727813196959203L;
    private String userId;
    private String channel;
    private String eventType;
    private long eventTime;
    private Product data;

}
