package com.ayx;


import com.alibaba.fastjson.JSON;
import com.ayx.dto.Message;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class CommitLogResolveUtil {

    public static String path = "C:\\Users\\Administrator\\store\\commitlog\\00000000000000000000";

    public static void main(String[] args) {
        try (FileChannel fileChannel = new RandomAccessFile(path, "rw").getChannel()) {

            MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel.size());
            //ByteBuffer byteBuffer = ByteBuffer.allocate((int) fileChannel.size());
            fileChannel.read(byteBuffer);
            byteBuffer.flip();
            while (byteBuffer.hasRemaining()) {
                //读取数据
                if (byteBuffer.remaining() < 4) {
                    System.out.println("数据头不完整，解析退出");
                    return;
                }
                int msgLen = byteBuffer.getInt();
                if ((byteBuffer.limit() - byteBuffer.position()) < msgLen || msgLen <= 0) {
                    System.out.println("数据解析完成");
                    return;
                }
                dataProcess(byteBuffer, msgLen);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Message dataProcess(ByteBuffer buffer, int msgLen) {

        int magicCode = buffer.getInt(); //MAGICCODE
        int bodyCRC = buffer.getInt(); //BODYCRC
        int queueId = buffer.getInt(); //QUEUEID
        int flag = buffer.getInt(); //FLAG
        long queueOffset = buffer.getLong(); //QUEUEOFFSET
        long phyOffset = buffer.getLong(); //PHYSICALOFFSET
        int sysFlag = buffer.getInt(); //SYSFLAG
        long bornTimestamp = buffer.getLong(); //BORNTIMESTAMP
        byte[] bornHostBytes = new byte[4]; //BORN HOST AND PORT 这里不知道为什么乱码

        buffer.get(bornHostBytes, 0, 4 );
        String bornHost = new String(bornHostBytes);
        int port = buffer.getInt();


        long storeTimestamp = buffer.getLong(); //STORETIMESTAMP
        byte[] storeHostByte = new byte[8]; //STOREHOSTADDRESS
        buffer.get(storeHostByte);


        int reconsumeTimes = buffer.getInt();//RECONSUMETIMES
        long preparedTransactionOffset = buffer.getLong(); //Prepared Transaction Offset

        int bodyLen = buffer.getInt(); //消息体长度
        String body = "";
        if (bodyLen > 0) {
            byte[] bodyBytes = new byte[bodyLen];
            buffer.get(bodyBytes);
            body = new String(bodyBytes);
        }
        String topic = "";
        byte topicLen = buffer.get(); //TOPICLEN
        if (topicLen > 0) {
            byte[] topicByte = new byte[topicLen];
            buffer.get(topicByte);
            topic = new String(topicByte);
        }

        short propertiesLen = buffer.getShort();
        String properties = "";
        if (propertiesLen > 0) {
            byte[] propertiesBytes = new byte[propertiesLen];
            buffer.get(propertiesBytes);
            properties = new String(propertiesBytes);
            properties = properties.replaceAll("[\\u0000-\\u001f\b]", " ");
        }

        return dataPackage(msgLen, magicCode, bodyCRC, queueId, flag, queueOffset, phyOffset, sysFlag, bornTimestamp, storeTimestamp, reconsumeTimes, preparedTransactionOffset, bodyLen, body, topicLen, topic, propertiesLen, properties);
    }

    private static Message dataPackage(int msgLen, int magicCode, int bodyCRC, int queueId, int flag, long queueOffset,
                                       long phyOffset, int sysFlag, long bornTimestamp, long storeTimestamp,
                                       int reconsumeTimes, long preparedTransactionOffset, int bodyLen,
                                       String body, byte topicLen, String topic, short propertiesLen, String properties) {
        Message message = new Message();
        message.setMsgLen(msgLen);
        message.setMagicCode(magicCode);
        message.setBodyCRC(bodyCRC);
        message.setQueueId(queueId);
        message.setFlag(flag);
        message.setQueueOffset(queueOffset);
        message.setPhyOffset(phyOffset);
        message.setSysFlag(sysFlag);
        message.setBornTimestamp(bornTimestamp);
        message.setStoreTimestamp(storeTimestamp);
        message.setReconsumeTimes(reconsumeTimes);
        message.setPreparedTransactionOffset(preparedTransactionOffset);
        message.setBodyLength(bodyLen);
        message.setBody(body);
        message.setTopicLength(topicLen);
        message.setTopic(topic);
        message.setPropertiesLength(propertiesLen);
        message.setPropertiesString(properties);
        System.out.println(JSON.toJSONString(message));
        System.out.println("====================");
        return message;
    }
}