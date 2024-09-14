package com.ayx.dto;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Message {
    private static final long serialVersionUID = 5720810158625748049L;
    private int msgLen;
    private int magicCode;
    private int bodyCRC;
    private int queueId;
    private int flag;
    private long queueOffset;
    private long phyOffset;
    private int sysFlag;
    private long bornTimestamp;
    private long storeTimestamp;
    private int reconsumeTimes;
    private long preparedTransactionOffset;
    private int bodyLength;
    private String body;
    private int topicLength;
    private String topic;
    private short propertiesLength;
    private String propertiesString;

}