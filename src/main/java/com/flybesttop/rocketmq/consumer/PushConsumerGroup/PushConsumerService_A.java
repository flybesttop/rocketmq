package com.flybesttop.rocketmq.consumer.PushConsumerGroup;

import com.flybesttop.rocketmq.config.RocketMQConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.UnsupportedEncodingException;

/**
 * @author flybesttop
 * @description: 测试消费者 A push
 */
@Component
public class PushConsumerService_A {

    @Value("${RocketMQ.nameServerAddr}")
    private String nameServerAddr;

    private DefaultMQPushConsumer mqPushConsumer;


    @PostConstruct
    public void initConsumer() throws MQClientException {
        mqPushConsumer = new DefaultMQPushConsumer(RocketMQConfig.TEST_CONSUMER_GROUP);
        mqPushConsumer.setNamesrvAddr(nameServerAddr);

        // 设置consumer所订阅的Topic和Tag，*代表全部的Tag
        mqPushConsumer.subscribe(RocketMQConfig.TEST_TOPIC, "*");

        // 消费者端消息过滤示例
//        mqPushConsumer.subscribe(RocketMQConfig.TEST_TOPIC, MessageSelector.bySql("test between 10 and 20"));

        // 消费策略
        // CONSUME_FROM_LAST_OFFSET 默认策略，从该队列最尾开始消费，跳过历史消息
        // CONSUME_FROM_FIRST_OFFSET 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
        mqPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        mqPushConsumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
            try {
                Message msg = list.get(0);
                System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(), "PushConsumerService_A", new String(list.get(0).getBody()));
                String topic = msg.getTopic();
                String body = new String(msg.getBody(), "utf-8");
                String tags = msg.getTags();
                String keys = msg.getKeys();
                System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
                // 成功就返回broker，让其进行删除
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (Exception e) {
                e.printStackTrace();
                // 失败就返回broker，让其稍后重试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        mqPushConsumer.start();
        System.out.println("Consumer_A start ...");
    }


    @PreDestroy
    public void destroy() {
        mqPushConsumer.shutdown();
    }

//    public ConsumerService() throws MQClientException {
//        mqPushConsumer = new DefaultMQPushConsumer(RocketMQConfig.TEST_CONSUMER_GROUP);
//        mqPushConsumer.setNamesrvAddr(nameServerAddr);
//        //配置消费策略，这个时从最后一个开始消费
//        mqPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
//        mqPushConsumer.subscribe("test_topic", "*");

//        mqPushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
//            try {
//                Message msg = msgs.get(0);
//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
//                String topic = msg.getTopic();
//                String body = new String(msg.getBody(), "utf-8");
//                String tags = msg.getTags();
//                String keys = msg.getKeys();
//                System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            } catch (UnsupportedEncodingException e) {
//                e.printStackTrace();
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//            }
//        });

//        mqPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
//                try {
//                    Message msg = list.get(0);
//                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(list.get(0).getBody()));
//                    String topic = msg.getTopic();
//                    String body = new String(msg.getBody(), "utf-8");
//                    String tags = msg.getTags();
//                    String keys = msg.getKeys();
//                    System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
//                    //成功就返回broker，让其进行删除
//                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                } catch (UnsupportedEncodingException e) {
//                    e.printStackTrace();
//                    //失败就返回broker，让其稍后重试
//                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//                }
//            }
//        });
//
//        mqPushConsumer.start();
//        System.out.println("consumer start ...");
//    }
}