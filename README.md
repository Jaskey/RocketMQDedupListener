# RocketMQDedupListener
通用的RocketMQ消息幂等去重消费者工具类，开箱即用

1. 支持利用Redis或者MySQL做幂等表。
2. 支持业务主键去重或消息ID去重（默认）
3. 支持消息重复并发控制（重复的消息消费成功/失败前，不会同时消费第二条）
4. 接近于EXACTLY-ONCE语义（消息只会且仅会被成功消费一次），极端场景下则为ATLEAST-ONCE语义（消息至少被成功消费一次，不会因为去重的增强而丢失消息）


# 内置去重原理

见以下流程图

![image](https://raw.githubusercontent.com/Jaskey/RocketMQDedupListener/master/dedup-flow.png)


# 去重实现的来龙去脉

可以参考本人在官方微信发表的博文： [RocketMQ消息幂等的通用解决方案](https://mp.weixin.qq.com/s/X25Jw-sz3XItVrXRS6IQdg) 

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
        //为了简单示意，这里直接使用消息体作为去重键，正式使用时候不建议这样使用
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

            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TEST-APP1");
            consumer.subscribe("TEST-TOPIC", "*");

            //START:区别于普通RocketMQ使用的代码
            String appName = consumer.getConsumerGroup();//针对什么应用做去重，相同的消息在不同应用的去重是隔离处理的
            StringRedisTemplate stringRedisTemplate = null;// 这里省略获取StringRedisTemplate的过程，具体的消息幂等表会保存到Redis中
            DedupConfig dedupConfig = DedupConfig.enableDedupConsumeConfig(appName, stringRedisTemplate);
            DedupConcurrentListener messageListener = new SampleListener(dedupConfig);
            //END:区别于普通RocketMQ使用的代码


            consumer.registerMessageListener(messageListener);
            consumer.start();
        
```

注：

1. 以上省略了RocketMQ消费者的配置及`StringRedisTemplate`的获取过程，需要使用者自己准备。
2. `RocketMQDedupListener`支持使用Redis或MySQL进行去重，更多使用详情请见`SampleListener.java`




# 使用场景测试

以下是部分实验的输出日志及现象，读者可以参考观察实验（基于`SampleListener.java`的实现）


## 一、测试普通的消息重复：

1. 模拟正常消息：发送消息到TEST-TOPIC， 报文为，test-ljj-msg1234
2. 模拟重复消息：隔几秒后（这个例子需要大于3秒），再发送消息到TEST-TOPIC，报文一样是test-ljj-msg1234



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


1. 模拟正常消息：发送消息到TEST-TOPIC， 报文为 test-ljj-msg123
2. 模拟重复消息：马上（这个例子需要小于3秒）再发送消息到TEST-TOPIC，报文一样是 test-ljj-msg123


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

------------------------

## MYSQL去重支持
若希望使用MYSQL存储消息消费记录，使用上仅需把StringRedisTemplate改成JdbcTemplate：

            JdbcTemplate jdbcTemplate = null;// 这里省略获取JDBCTemplate的过程，幂等表将使用MySQL的t_rocketmq_dedup存储
            DedupConfig dedupConfig = DedupConfig.enableDedupConsumeConfig(appName, jdbcTemplate);



同时需要预先建立一张消息去重表，结构如下:

```
-- ----------------------------
-- Table structure for t_rocketmq_dedup
-- ----------------------------
DROP TABLE IF EXISTS `t_rocketmq_dedup`;
CREATE TABLE `t_rocketmq_dedup` (
`application_name` varchar(255) NOT NULL COMMENT '消费的应用名（可以用消费者组名称）',
`topic` varchar(255) NOT NULL COMMENT '消息来源的topic（不同topic消息不会认为重复）',
`tag` varchar(16) NOT NULL COMMENT '消息的tag（同一个topic不同的tag，就算去重键一样也不会认为重复），没有tag则存""字符串',
`msg_uniq_key` varchar(255) NOT NULL COMMENT '消息的唯一键（建议使用业务主键）',
`status` varchar(16) NOT NULL COMMENT '这条消息的消费状态',
`expire_time` bigint(20) NOT NULL COMMENT '这个去重记录的过期时间（时间戳）',
UNIQUE KEY `uniq_key` (`application_name`,`topic`,`tag`,`msg_uniq_key`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;


```

说明:因为需要支持不同的应用，所以需要存储application_name，因为同一个业务主键可能来自不同的topic/tag，所以也需要存储起来。


### 一直消费失败会否引起死循环

不会。失败/消费中触发的延迟消费依赖与RocketMQ原生的重试机制，默认是16次。如果有希望调整延迟的时间和重试次数，需要自行调整Consumer配置。

# 关于作者

Apache RocketMQ Committer，知乎专栏 [RocketMQ详解](https://zhuanlan.zhihu.com/rocketmq)作者，RoceketMQ官微投稿者
