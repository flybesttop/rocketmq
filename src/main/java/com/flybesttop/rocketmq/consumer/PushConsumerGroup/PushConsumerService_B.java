package com.flybesttop.rocketmq.consumer.PushConsumerGroup;

import com.flybesttop.rocketmq.config.RocketMQConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
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
 * @description 测试消费者 B push
 * @author flybesttop
 */
@Component
public class PushConsumerService_B {

    @Value("${RocketMQ.nameServerAddr}")
    private String nameServerAddr;

    private DefaultMQPushConsumer mqPushConsumer;


    @PostConstruct
    public void initConsumer() throws MQClientException {
        mqPushConsumer = new DefaultMQPushConsumer(RocketMQConfig.TEST_CONSUMER_GROUP);
        mqPushConsumer.setNamesrvAddr(nameServerAddr);

        // 设置consumer所订阅的Topic和Tag，*代表全部的Tag
        mqPushConsumer.subscribe(RocketMQConfig.TEST_TOPIC, "*");

        // 消费策略
        // CONSUME_FROM_LAST_OFFSET 默认策略，从该队列最尾开始消费，跳过历史消息
        // CONSUME_FROM_FIRST_OFFSET 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
        mqPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        mqPushConsumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
            try {
                Message msg = list.get(0);
                System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(), "PushConsumerService_B", new String(list.get(0).getBody()));
                String topic = msg.getTopic();
                String body = new String(msg.getBody(), "utf-8");
                String tags = msg.getTags();
                String keys = msg.getKeys();
                System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
                // 成功就返回broker，让其进行删除
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                // 失败就返回broker，让其稍后重试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        mqPushConsumer.start();
        System.out.println("Consumer_B start ...");
    }


    @PreDestroy
    public void destroy() {
        mqPushConsumer.shutdown();
    }
}
