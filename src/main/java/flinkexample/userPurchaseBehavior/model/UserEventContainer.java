package flinkexample.userPurchaseBehavior.model;

import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * 用户事件列表对象
 *
 * @author dajiangtai
 * @create 2019-06-24-14:34
 */
@Data
@ToString
public class UserEventContainer {
    private String userId;
    private List<UserEvent> userEvents = new ArrayList<>();
}
