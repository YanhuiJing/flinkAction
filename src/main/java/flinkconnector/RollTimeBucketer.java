package flinkconnector;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;


public class RollTimeBucketer<T> implements Bucketer<T> {


    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";
    private static final long DEFAULT_ROLL_INTERVAL = 1;

    private final String formatString;
    private final ZoneId zoneId;
    /**
     * 滚动周期，ms
     */
    private final long rollInterval;

    private transient DateTimeFormatter dateTimeFormatter;

    public RollTimeBucketer() {
        this(DEFAULT_FORMAT_STRING, ZoneId.systemDefault(), DEFAULT_ROLL_INTERVAL, TimeUnit.HOURS);
    }

    public RollTimeBucketer(String formatString, long rollInterval, TimeUnit timeUnit) {
        this(formatString, ZoneId.systemDefault(), rollInterval, timeUnit);
    }

    /**
     * 创建 RollTimeBucketer
     *
     * @param formatString 用于桶名称的日期格式化
     * @param zoneId       格式化时区
     * @param rollInterval 滚动的时间间隔
     * @param timeUnit     时间单位，enum 类型
     */
    public RollTimeBucketer(String formatString, ZoneId zoneId, long rollInterval, TimeUnit timeUnit) {
        this.formatString = Preconditions.checkNotNull(formatString);
        this.zoneId = Preconditions.checkNotNull(zoneId);
        this.rollInterval = timeUnit.toMillis(rollInterval);

        this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.formatString).withZone(zoneId);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        this.dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
    }

    @Override
    public Path getBucketPath(Clock clock, Path basePath, T element) {
        long time = clock.currentTimeMillis() / this.rollInterval * this.rollInterval;
        String newDateTimeString = dateTimeFormatter.format(Instant.ofEpochMilli(time));
        return new Path(basePath + "/" + newDateTimeString);
    }

    @Override
    public String toString() {
        return "RollTimeBucketer{" +
                "formatString='" + formatString + '\'' +
                ", zoneId=" + zoneId +
                ", rollInterval=" + rollInterval +
                '}';
    }

}
