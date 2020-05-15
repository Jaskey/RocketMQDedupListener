package com.github.jaskey.rocketmq.persist;


import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;

import java.util.concurrent.TimeUnit;

/**
 * Created by linjunjie1103@gmail.com
 */

public class RedisPersist implements IPersist {
    private final StringRedisTemplate redisTemplate;

    public RedisPersist(StringRedisTemplate redisTemplate) {
        if (redisTemplate == null) {
            throw new NullPointerException("redis template is null");
        }
        this.redisTemplate = redisTemplate;
    }



    @Override
    public boolean setConsumingIfNX(DedupElement dedupElement, long dedupProcessingExpireMilliSeconds) {
        String dedupKey = buildDedupMessageRedisKey(dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey());

        //setnx, 成功就可以消费
        Boolean execute = redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> redisConnection.set(dedupKey.getBytes(), (CONSUME_STATUS_CONSUMING).getBytes(), Expiration.milliseconds(dedupProcessingExpireMilliSeconds), RedisStringCommands.SetOption.SET_IF_ABSENT));

        if (execute == null) {
            return false;
        }

        return execute;
    }

    @Override
    public void delete(DedupElement dedupElement) {
        String dedupKey = buildDedupMessageRedisKey(dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey());

        redisTemplate.delete(dedupKey);
    }

    @Override
    public void markConsumed(DedupElement dedupElement, long dedupRecordReserveMinutes) {
        String dedupKey = buildDedupMessageRedisKey(dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey());

        redisTemplate.opsForValue().set(dedupKey, CONSUME_STATUS_CONSUMED, dedupRecordReserveMinutes, TimeUnit.MINUTES);

    }

    @Override
    public String get(DedupElement dedupElement) {
        String dedupKey = buildDedupMessageRedisKey(dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey());
        return redisTemplate.opsForValue().get(dedupKey);
    }


    private  String buildDedupMessageRedisKey(String applicationName, String topic, String tag, String msgUniqKey) {
        if (StringUtils.isEmpty(msgUniqKey)) {
            return null;
        } else {
            //示例：MSGDEDUP:APPNAME:TOPIC:TAG:APP_DEDUP_KEY
            String prefix = "MSGDEDUP:" + applicationName + ":" + topic + (StringUtils.isNotEmpty(tag) ? ":"+ tag :"");
            return prefix + ":" + msgUniqKey;
        }
    }


}
