package com.github.jaskey.rocketmq.strategy;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.function.Function;

/**
 * Created by linjunjie1103@gmail.com
 */
public interface ConsumeStrategy {
     boolean invoke(Function<MessageExt, Boolean> consumeCallback, MessageExt messageExt);
}

