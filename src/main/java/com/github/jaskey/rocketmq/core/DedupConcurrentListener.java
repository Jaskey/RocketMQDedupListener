package com.github.jaskey.rocketmq.core;



import com.github.jaskey.rocketmq.strategy.ConsumeStrategy;
import com.github.jaskey.rocketmq.strategy.DedupConsumeStrategy;
import com.github.jaskey.rocketmq.strategy.NormalConsumeStrategy;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.function.Function;


/**
 * Created by linjunjie1103@gmail.com
 * 带去重逻辑的通用消费者，实现者需要实现doHandleMsg
 * 支持消息幂等的策略
 */
@Slf4j
public abstract class DedupConcurrentListener implements MessageListenerConcurrently {

    // 默认不去重
    private DedupConfig dedupConfig = DedupConfig.disableDupConsumeConfig("NOT-SET-CONSUMER-GROUP");

    /**
     * 默认策略不去重
     */
    private ConsumeStrategy strategy = new NormalConsumeStrategy();

    /**
     * 默认不去重
     */
    public DedupConcurrentListener(){
        log.info("Construct QBConcurrentRMQListener with default {}", dedupConfig);
    }

    /**
     * 设置去重策略
     * @param dedupConfig
     */
    public DedupConcurrentListener(DedupConfig dedupConfig) {
        this.dedupConfig = dedupConfig;
        if (dedupConfig.getDedupStrategy() == DedupConfig.DEDUP_STRATEGY_CONSUME_LATER) {
            strategy = new DedupConsumeStrategy(dedupConfig, this::dedupMessageKey);
        }
        log.info("Construct QBConcurrentRMQListener with dedupConfig {}", dedupConfig);
    }


    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        boolean hasConsumeFail = false;
        int ackIndexIfFail = -1;
        for (int i = 0; i < msgs.size(); i++) {
            MessageExt msg = msgs.get(i);
            try {
                hasConsumeFail = !handleMsgInner(msg);
            } catch (Exception ex) {
                log.warn("Throw Exception when consume {}, ex", msg, ex);
                hasConsumeFail = true;
            }

            //如果前面出现消费失败的话，后面也不用消费了，因为都会重发
            if (hasConsumeFail) {
                break;
            } else { //到现在都消费成功
                ackIndexIfFail = i;
            }
        }

        if (!hasConsumeFail) {//全都消费成功
            log.info("consume [{}] msg(s) all successfully", msgs.size());
        } else {//存在失败的
            context.setAckIndex(ackIndexIfFail);//标记成功位，后面的会重发以重新消费，在这个位置之前的不会重发。 详情见源码：ConsumeMessageConcurrentlyService#processConsumeResult
            log.warn("consume [{}] msg(s) fails, ackIndex = [{}] ", msgs.size(), context.getAckIndex());
        }


        //无论如何最后都返回成功
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * 子类实现此方法。真正处理消息
     * @param messageExt
     * @return true表示消费成功，false表示消费失败
     */
    protected abstract boolean doHandleMsg(final MessageExt messageExt);


    /**
     *  默认拿uniqkey 作为去重的标识
     */
    protected String dedupMessageKey(final MessageExt messageExt) {
        String uniqID = MessageClientIDSetter.getUniqID(messageExt);
        if (uniqID == null) {
            return messageExt.getMsgId();
        } else {
            return uniqID;
        }
    }




    //消费消息，带去重的逻辑
    private boolean handleMsgInner(final MessageExt messageExt) {
        //调用对应的策略
        return strategy.invoke(DedupConcurrentListener.this::doHandleMsg, messageExt);
    }



}



