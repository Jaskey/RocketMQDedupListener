package com.github.jaskey.rocketmq.persist;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * Created by linjunjie1103@gmail.com
 */
@AllArgsConstructor
@Getter
@ToString
public class DedupElement {
    private String application;
    private String topic;
    private String tag;
    private String msgUniqKey;

}
