package com.github.jaskey.rocketmq;

import com.github.jaskey.rocketmq.core.DedupConcurrentListener;
import com.github.jaskey.rocketmq.core.DedupConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
public class SampleListener extends DedupConcurrentListener {

    public SampleListener(DedupConfig dedupConfig) {
        super(dedupConfig);
    }

    //基于什么做消息去重，每一类不同的消息都可以不一样，做去重之前会尊重此方法返回的值
    @Override
    protected String dedupMessageKey(MessageExt messageExt) {
        //为了简单示意，这里直接使用消息体作为去重键
        if ("TEST-TOPIC".equals(messageExt.getTopic())) {
            return new String(messageExt.getBody());
        } else {//其他使用默认的配置（消息id）
            return super.dedupMessageKey(messageExt);
        }
    }

    @Override
    protected boolean doHandleMsg(MessageExt messageExt) {
        switch (messageExt.getTopic()) {
            case "TEST-TOPIC":
                log.info("假装消费很久....{} {}", new String(messageExt.getBody()), messageExt);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {}
                break;
        }
        return true;
    }

    public static void main(String[] args) throws MQClientException {


        //利用Redis做幂等表
        {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TEST-APP1");
            consumer.subscribe("TEST-TOPIC", "*");

            String appName = consumer.getConsumerGroup();
            StringRedisTemplate stringRedisTemplate = null;// 这里省略获取StringRedisTemplate的过程
            DedupConfig dedupConfig = DedupConfig.enableDedupConsumeConfig(appName, stringRedisTemplate);
            DedupConcurrentListener messageListener = new SampleListener(dedupConfig);

            consumer.registerMessageListener(messageListener);
            consumer.start();
        }

        //利用MySQL做幂等表
        {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TEST-APP2");
            consumer.subscribe("TEST-TOPIC", "*");


            String appName = consumer.getConsumerGroup();
            JdbcTemplate jdbcTemplate = null;// 这里省略获取JDBCTemplate的过程
            DedupConfig dedupConfig = DedupConfig.enableDedupConsumeConfig(appName, jdbcTemplate);
            DedupConcurrentListener messageListener = new SampleListener(dedupConfig);

            consumer.registerMessageListener(messageListener);
            consumer.start();
        }
    }

}
