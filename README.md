# RocketMQDedupListener
通用的RocketMQ消息幂等去重消费者工具类


# Quick Start

### 1、继承`DedupConcurrentListener`类，实现消费回调和去重键的设置回调

```
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
}
```

### 2、使用此实例启动RocketMQ 消费者

```

            //利用Redis做幂等表
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TEST-APP1");
            consumer.subscribe("TEST-TOPIC", "*");

            String appName = consumer.getConsumerGroup();
            StringRedisTemplate stringRedisTemplate = null;// 这里省略获取StringRedisTemplate的过程
            DedupConfig dedupConfig = DedupConfig.enableDedupConsumeConfig(appName, stringRedisTemplate);
            DedupConcurrentListener messageListener = new SampleListener(dedupConfig);

            consumer.registerMessageListener(messageListener);
            consumer.start();
        
```

注：

1. 以上省略了RocketMQ消费者的配置及`StringRedisTemplate`的获取过程，需要使用者自己准备。
2. `RocketMQDedupListener`支持使用Redis或MySQL进行去重，更多使用详情请见`SampleListener.java`




# 使用场景测试

以下是部分实验的输出日志及现象，读者可以参考观察实验（基于`SampleListener.java`的实现）


## 一、测试普通的消息重复：

1.模拟正常消息：发送消息到TEST-TOPIC， 报文为，test-ljj-msg1234
2.模拟重复消息：隔几秒后（这个例子需要大于3秒），再发送消息到TEST-TOPIC，报文一样是test-ljj-msg1234



### 日志输出如下：


```
[INFO] 2020-05-15 11:06:17,697 []  >>> 假装消费很久....test-ljj-msg1234 MessageExt [queueId=1, storeSize=169, queueOffset=0, sysFlag=0, bornTimestamp=1589511454575, bornHost=/10.13.32.179:52637, storeTimestamp=1589511454576, storeHost=/10.13.32.179:10911, msgId=0A0D20B300002A9F000000003EEA31B0, commitLogOffset=1055535536, bodyCRC=1038040938, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message [topic=TEST-TOPIC, flag=0, properties={MIN_OFFSET=0, MAX_OFFSET=1, CONSUME_START_TIME=1589511977328, UNIQ_KEY=0A0D20B3632A5B2133B14A730F6F014A, WAIT=true}, body=16]]
[INFO] 2020-05-15 11:06:20,748 []  >>> consume [1] msg(s) all successfully
[WARN] 2020-05-15 11:06:26,504 []  >>> message has been consumed before! dedupKey = DedupElement={"application":"repay-platform-core","msgUniqKey":"test-ljj-msg1234","tag":"","topic":"TEST-TOPIC"}, msgId : 0A0D20B3632A5B2133B14A7332DB014B , so just ack. RedisPersist
[INFO] 2020-05-15 11:06:26,504 []  >>> consume [1] msg(s) all successfully
```


说明：

可以看到第二条消息被直接幂等掉了，没有进入业务的测试代码



## 测试并发重复消费：


1. 模拟正常消息：发送消息到TEST-TOPIC， 报文为，.test-ljj-msg123
2. 模拟重复消息：马上（这个例子需要小于3秒）再发送消息到TEST-TOPIC，报文一样是.test-ljj-msg123


由于这里需要一些特殊说明，以下日志增加了注释


```
33秒第一条消息到达，这里消息会消费3秒
[INFO] 2020-05-15 11:07:33,756 []  >>> 假装消费很久....test-ljj-msg123 MessageExt [queueId=1, storeSize=168, queueOffset=2, sysFlag=0, bornTimestamp=1589511530879, bornHost=/10.13.32.179:52651, storeTimestamp=1589511530881, storeHost=/10.13.32.179:10911, msgId=0A0D20B300002A9F000000003EEA3302, commitLogOffset=1055535874, bodyCRC=146853239, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message [topic=TEST-TOPIC, flag=0, properties={MIN_OFFSET=0, MAX_OFFSET=3, CONSUME_START_TIME=1589512053623, UNIQ_KEY=0A0D20B3632A5B2133B14A74397F014C, WAIT=true}, body=15]]

35秒重复消息到达，发现前面的消息还在消费
[WARN] 2020-05-15 11:07:35,884 []  >>> the same message is considered consuming, try consume later dedupKey : DedupElement={"application":"repay-platform-core","msgUniqKey":"test-ljj-msg123","tag":"","topic":"TEST-TOPIC"}, 0A0D20B3632A5B2133B14A7441FB014D, RedisPersit
消费按消费失败处理，触发延迟消费
[WARN] 2020-05-15 11:07:35,884 []  >>> consume [1] msg(s) fails, ackIndex = [-1]

36秒第一条消息消费成功
[INFO] 2020-05-15 11:07:36,801 []  >>> consume [1] msg(s) all successfully

46秒第二条消息延迟消费开始，发现这条消息已经被成功消费
[WARN] 2020-05-15 11:07:46,024 []  >>> message has been consumed before! dedupKey = DedupElement={"application":"repay-platform-core","msgUniqKey":"test-ljj-msg123","tag":"","topic":"TEST-TOPIC"}, msgId : 0A0D20B3632A5B2133B14A7441FB014D , so just ack. RedisPersit

直接按照消费成功处理
[INFO] 2020-05-15 11:07:46,024 []  >>> consume [1] msg(s) all successfully
```


说明：

可以看到第二条消息在第一条消息消费的过程中就投递到消费者了，这时候去重逻辑做了并发控制，保证了业务代码的安全。


