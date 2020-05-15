package com.github.jaskey.rocketmq.persist;

/**
 * Created by linjunjie1103@gmail.com
 */
public interface IPersist {

     String CONSUME_STATUS_CONSUMING = "CONSUMING";
     String CONSUME_STATUS_CONSUMED = "CONSUMED";


    boolean setConsumingIfNX(DedupElement dedupElement, long dedupProcessingExpireMilliSeconds);

    void delete(DedupElement dedupElement);

    void markConsumed(DedupElement dedupElement, long dedupRecordReserveMinutes);

    String get(DedupElement dedupElement);
}
