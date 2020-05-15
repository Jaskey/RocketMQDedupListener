package com.github.jaskey.rocketmq.persist;


import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;


import java.util.Map;

/**
 * Created by linjunjie1103@gmail.com
 */
@Slf4j
public class JDBCPersit implements IPersist {
    private final JdbcTemplate jdbcTemplate;

    public JDBCPersit(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean setConsumingIfNX(DedupElement dedupElement, long dedupProcessingExpireMilliSeconds) {
        long expireTime = System.currentTimeMillis() + dedupProcessingExpireMilliSeconds;
        try {
            int  i = jdbcTemplate.update("INSERT INTO t_rocketmq_dedup(application_name, topic, tag, msg_uniq_key, status, expire_time) values (?, ?, ?, ?, ?, ?)",dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey(), CONSUME_STATUS_CONSUMING, expireTime);
        } catch (org.springframework.dao.DuplicateKeyException e) {
            log.warn("found consuming/consumed record, set setConsumingIfNX fail {}", dedupElement);


            /**
             * 由于mysql不支持消息过期，出现重复主键的情况下，有可能是过期的一些记录，这里动态的删除这些记录后重试
             */
            int  i = delete(dedupElement, true);
            if (i > 0) {//如果删除了过期消息
                log.info("delete {} expire records, now retry setConsumingIfNX again", i);
                return setConsumingIfNX(dedupElement, dedupProcessingExpireMilliSeconds);
            } else {
                return false;
            }
        } catch (Exception e) {
            log.error("unknown error when jdbc insert, will consider success", e);
            return true;
        }

        //插入成功则返回true
        return true;
    }


    private int delete (DedupElement dedupElement, boolean onlyExpire) {
        if (onlyExpire) {
            return jdbcTemplate.update("DELETE FROM t_rocketmq_dedup  WHERE application_name = ? AND topic =? AND tag = ? AND msg_uniq_key = ? AND expire_time < ?", dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey(), System.currentTimeMillis());
        } else {
            return jdbcTemplate.update("DELETE FROM t_rocketmq_dedup  WHERE application_name = ? AND topic =? AND tag = ? AND msg_uniq_key = ?", dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey());
        }
    }
    @Override
    public void delete(DedupElement dedupElement) {
        delete(dedupElement, false);
    }


    @Override
    public void markConsumed(DedupElement dedupElement, long dedupRecordReserveMinutes) {
        long expireTime = System.currentTimeMillis() + dedupRecordReserveMinutes * 60 * 1000;
        int  i = jdbcTemplate.update("UPDATE t_rocketmq_dedup SET status = ? , expire_time  = ? WHERE application_name = ? AND topic = ? AND tag = ? AND msg_uniq_key = ? ",
                CONSUME_STATUS_CONSUMED, expireTime, dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey());
    }

    @Override
    public String get(DedupElement dedupElement) {
        Map<String, Object> res =jdbcTemplate.queryForMap("SELECT status FROM t_rocketmq_dedup where application_name = ? AND topic = ? AND tag = ? AND msg_uniq_key  = ? and expire_time > ?",
                dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey(), System.currentTimeMillis());
        return (String)res.get("status");
    }


}
