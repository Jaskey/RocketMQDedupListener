package com.github.jaskey.rocketmq.core;


import com.github.jaskey.rocketmq.persist.IPersist;
import com.github.jaskey.rocketmq.persist.JDBCPersit;
import com.github.jaskey.rocketmq.persist.RedisPersist;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.function.Function;

@Getter
@ToString
public class DedupConfig {

    /**
     * 不启用去重
     */
    public static final int DEDUP_STRATEGY_DISABLE = 0;
    /**
     * 开启去重，发现有处理中的消息，后面再重试
     */
    public static final int DEDUP_STRATEGY_CONSUME_LATER = 1;
//  public static final int DEDUP_STRATEGY_DROP = 2; //直接当重复处理


    private String applicationName;//用以标记去重的时候是哪个应用消费的，同一个应用才需要去重

    private IPersist persist;


    //去重策略，默认不去重
    private int dedupStrategy = DEDUP_STRATEGY_DISABLE;


    /**
     * 对于消费中的消息，多少毫秒内认为重复，默认一分钟，即一分钟内的重复消息都会串行处理（等待前一个消息消费成功/失败），超过这个时间如果消息还在消费就不认为重复了（为了防止消息丢失）
     */
    private long dedupProcessingExpireMilliSeconds = 60 * 1000;

    /**
     * 消息消费成功后，记录保留多少分钟，默认一天，即一天内的消息不会重复
     */
    private long dedupRecordReserveMinutes = 60 * 24;

    private int degrationTimes = 10000;

    //默认拿uniqkey 作为去重的标识
    public static Function<MessageExt, String> defaultDedupMessageKeyFunction = messageExt -> {
        String uniqID = MessageClientIDSetter.getUniqID(messageExt);
        if (uniqID == null) {
            return messageExt.getMsgId();
        } else {
            return uniqID;
        }
    };



    private DedupConfig(String applicationName, int dedupStrategy, StringRedisTemplate redisTemplate) {
        if (redisTemplate !=null) {
            this.persist = new RedisPersist(redisTemplate);
        }
        this.dedupStrategy = dedupStrategy;
        this.applicationName = applicationName;
    }

    private DedupConfig(String applicationName, int dedupStrategy, JdbcTemplate jdbcTemplate) {
        if (jdbcTemplate !=null) {
            this.persist = new JDBCPersit(jdbcTemplate);
        }
        this.dedupStrategy = dedupStrategy;
        this.applicationName = applicationName;
    }

    private DedupConfig(String applicationName) {
        this.dedupStrategy = DEDUP_STRATEGY_DISABLE;
        this.applicationName = applicationName;
    }


    /**
     * 利用redis去重
     * @param applicationName
     * @param redisTemplate
     * @return
     */
    public static DedupConfig enableDedupConsumeConfig(String applicationName, StringRedisTemplate redisTemplate) {
        return new DedupConfig(applicationName, DEDUP_STRATEGY_CONSUME_LATER, redisTemplate);
    }

    /**
     * 利用mysql去重
     * @param applicationName
     * @param jdbcTemplate
     * @return
     */
    public static DedupConfig enableDedupConsumeConfig(String applicationName, JdbcTemplate jdbcTemplate) {
        return new DedupConfig(applicationName, DEDUP_STRATEGY_CONSUME_LATER, jdbcTemplate);
    }

    public static DedupConfig disableDupConsumeConfig(String applicationName) {
        return new DedupConfig(applicationName);
    }



    public void setDedupProcessingExpireMilliSeconds(long dedupProcessingExpireMilliSeconds) {
        this.dedupProcessingExpireMilliSeconds = dedupProcessingExpireMilliSeconds;
    }

    public void setDedupRecordReserveMinutes(long dedupRecordReserveMinutes) {
        this.dedupRecordReserveMinutes = dedupRecordReserveMinutes;
    }

    public void setDegrationTimes(int degrationTimes) {
        this.degrationTimes = degrationTimes;
    }
}