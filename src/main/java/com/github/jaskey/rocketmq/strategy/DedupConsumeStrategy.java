package com.github.jaskey.rocketmq.strategy;


import com.github.jaskey.rocketmq.core.DedupConfig;
import com.github.jaskey.rocketmq.persist.DedupElement;
import com.github.jaskey.rocketmq.persist.IPersist;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.function.Function;

import static com.github.jaskey.rocketmq.persist.IPersist.CONSUME_STATUS_CONSUMED;
import static com.github.jaskey.rocketmq.persist.IPersist.CONSUME_STATUS_CONSUMING;


/**
 * Created by linjunjie1103@gmail.com
 * 去重策略的消费策略，去重数据存储目前支持MySQL（JDBC）和Redis，详见 persist包下的实现类
 * 1.如果已经消费过，则直接消费幂等掉
 * 2.如果正在消费中，则消费会延迟消费（consume later）注：如果一直消费中，由于需要避免消息丢失，即使前一个消息没消费结束依然会消费
 *
 */
@Slf4j
@AllArgsConstructor
public class DedupConsumeStrategy implements ConsumeStrategy {



    private final DedupConfig dedupConfig;

    //获取去重键的函数
    private final Function<MessageExt, String> dedupMessageKeyFunction;



    @Override
    public boolean invoke(Function<MessageExt, Boolean> consumeCallback, MessageExt messageExt) {
        return doInvoke(consumeCallback, messageExt);
    }


    private boolean doInvoke(Function<MessageExt, Boolean> consumeCallback, MessageExt messageExt) {

        IPersist persist = dedupConfig.getPersist();
        DedupElement dedupElement = new DedupElement(dedupConfig.getApplicationName(), messageExt.getTopic(), messageExt.getTags()==null ? "" : messageExt.getTags(), dedupMessageKeyFunction.apply(messageExt));
        Boolean shouldConsume = true;

        if (dedupElement.getMsgUniqKey() != null) {
            shouldConsume = persist.setConsumingIfNX(dedupElement, dedupConfig.getDedupProcessingExpireMilliSeconds());
        }

        //设置成功，证明应该要消费
        if (shouldConsume != null && shouldConsume) {
            //开始消费
            return doHandleMsgAndUpdateStatus(consumeCallback,messageExt, dedupElement);
        } else {//有消费过/中的，做对应策略处理
            String val = persist.get(dedupElement);
            if (CONSUME_STATUS_CONSUMING.equals(val)) {//正在消费中，稍后重试
                log.warn("the same message is considered consuming, try consume later dedupKey : {}, {}, {}", dedupElement, messageExt.getMsgId(), persist.getClass().getSimpleName());
                return false;
            } else if(CONSUME_STATUS_CONSUMED.equals(val)){//证明消费过了，直接消费认为成功
                log.warn("message has been consumed before! dedupKey = {}, msgId : {} , so just ack. {}", dedupElement, messageExt.getMsgId(), persist.getClass().getSimpleName());
                return true;
            } else {//redis值不可知，降级，直接消费
                log.warn("[NOTIFYME]未知的REDIS值，忽略去重结果，仍然执行消费 dedupKey {}, {}, {} ",dedupElement, messageExt.getMsgId(), persist.getClass().getSimpleName());
                return doHandleMsgAndUpdateStatus(consumeCallback,messageExt, dedupElement);
            }
        }
    }


    /**
     *     消费消息，末尾消费失败会删除消费记录，消费成功则更新消费状态
     */
    private boolean doHandleMsgAndUpdateStatus(final Function<MessageExt, Boolean> consumeCallback , final MessageExt messageExt, final DedupElement dedupElement) {


        if (dedupElement.getMsgUniqKey()==null) {
            log.warn("dedup key is null , consume msg but not update status{}", messageExt.getMsgId());
            return consumeCallback.apply(messageExt);
        } else {
            IPersist persist = dedupConfig.getPersist();
            boolean consumeRes = false;
            try {
                consumeRes = consumeCallback.apply(messageExt);
            } catch (Throwable e) {
                //消费失败了，删除这个key
                try {
                    persist.delete(dedupElement);
                } catch (Exception ex) {
                    log.error("error when delete dedup record {}", dedupElement, ex);
                }
                throw e;
            }


            //没有异常，正常返回的话，判断消费结果
            try {
                if (consumeRes) {//标记为这个消息消费过
                    log.debug("set consume res as CONSUME_STATUS_CONSUMED , {}", dedupElement);
                    persist.markConsumed(dedupElement, dedupConfig.getDedupRecordReserveMinutes());
                } else {
                    log.info("consume Res is false, try deleting dedup record {} , {}", dedupElement, persist);
                    persist.delete(dedupElement);//消费失败了，删除这个key
                }
            } catch (Exception e) {
                log.error("消费去重收尾工作异常 {}，忽略异常", messageExt.getMsgId(), e);
            }
            return consumeRes;
        }

    }
}