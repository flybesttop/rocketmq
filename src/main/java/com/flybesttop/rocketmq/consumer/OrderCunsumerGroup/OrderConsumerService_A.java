package com.flybesttop.rocketmq.consumer.OrderCunsumerGroup;

import com.flybesttop.rocketmq.config.RocketMQConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Random;

/**
 * 顺序消费 消费者 A
 * @author mtx
 */
@Component
public class OrderConsumerService_A {

    @Value("${RocketMQ.nameServerAddr}")
    private String nameServerAddr;

    private DefaultMQPushConsumer mqPushConsumer;

    @PostConstruct
    public void initConsumer() throws MQClientException {
        mqPushConsumer = new DefaultMQPushConsumer(RocketMQConfig.ORDER_CONSUMER_GROUP);
        mqPushConsumer.setNamesrvAddr(nameServerAddr);

        // 设置consumer所订阅的Topic和Tag，*代表全部的Tag
        mqPushConsumer.subscribe(RocketMQConfig.ORDER_TOPIC, "*");

        // 消费策略
        // CONSUME_FROM_LAST_OFFSET 默认策略，从该队列最尾开始消费，跳过历史消息
        // CONSUME_FROM_FIRST_OFFSET 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
        mqPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        mqPushConsumer.registerMessageListener((MessageListenerOrderly) (list, context) -> {
            try {
                //模拟业务处理消息的时间
                try {
                    Thread.sleep(new Random().nextInt(1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(), "OrderConsumerService_A", new String(list.get(0).getBody()));
//                Message msg = list.get(0);
//                String topic = msg.getTopic();
//                String body = new String(msg.getBody(), "utf-8");
//                String tags = msg.getTags();
//                String keys = msg.getKeys();
//                System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
                // 成功就返回broker，让其进行删除
                return ConsumeOrderlyStatus.SUCCESS;
            } catch (Exception e) {
                e.printStackTrace();
                // 失败就返回broker，让其稍后重试
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        });
        mqPushConsumer.start();
        System.out.println("OrderConsumer_A start ...");
    }


    @PreDestroy
    public void destroy() {
        mqPushConsumer.shutdown();
    }
}
