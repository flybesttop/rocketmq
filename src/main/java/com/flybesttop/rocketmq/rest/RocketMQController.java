package com.flybesttop.rocketmq.rest;

import com.flybesttop.rocketmq.config.RocketMQConfig;
import com.flybesttop.rocketmq.producer.OrderProducer;
import com.flybesttop.rocketmq.producer.DefaultProducer;
import com.flybesttop.rocketmq.dto.BaseResponse;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author flybesttop
 * @description RocketMQ测试
 */
@RestController
@RequestMapping("rocketMQ")
public class RocketMQController {

    @Autowired
    private DefaultProducer defaultProducer;

    @Autowired
    private OrderProducer orderProducerA;

    /**
     * 发送同步，非顺序消息
     * 使用场景：比较重要的通知，对实时性要求比较高的需求
     *
     * @param text
     * @return
     */
    @RequestMapping("syncProducerMessage")
    public BaseResponse<String> syncProducerMessage(String text) {
        try {
            for (int i = 1; i <= 100; i++) {
                String sendText = "No." + i + " value:" + text;
                Message message = new Message(RocketMQConfig.TEST_TOPIC, "syncMessage", sendText.getBytes());
                // 过滤器
//                message.putUserProperty("test", String.valueOf(i));
                SendResult res = defaultProducer.getMqProducer().send(message);
                System.out.println(res);
            }
        } catch (Exception e) {
            return new BaseResponse<>("消息发送失败");
        }
        return new BaseResponse<>("消息发送成功");
    }

    /**
     * 发送异步，非顺序消息
     * 使用场景：对时间较为敏感的业务场景可以用于流量削峰
     *
     * @param text
     * @return
     */
    @RequestMapping("asyncProducerMessage")
    public BaseResponse<String> asyncProducerMessage(String text) throws InterruptedException {
        Message message = new Message(RocketMQConfig.TEST_TOPIC, "asyncMessage", text.getBytes());
        DefaultMQProducer producer = this.defaultProducer.getMqProducer();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        int messageCount = 1000;
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;
                producer.send(message, new SendCallback() {

                    @Override
                    public void onSuccess(SendResult sendResult) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        countDownLatch.countDown();
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable e) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                return new BaseResponse<>("消息发送失败");
            }
        }
        countDownLatch.await(5, TimeUnit.SECONDS);
        return new BaseResponse<>("消息发送成功");
    }

    /**
     * 单向消息发送
     * 对于一些，不需要返回的场景，比如日志记录
     *
     * @param text
     * @return
     */
    @RequestMapping("oneWayProducerMessage")
    public BaseResponse<String> oneWayProducerMessage(String text) {
        try {
            for (int i = 1; i <= 100; i++) {
                String sendText = "No." + i + " value:" + text;
                Message message = new Message(RocketMQConfig.TEST_TOPIC, "syncMessage", sendText.getBytes());
                defaultProducer.getMqProducer().sendOneway(message);
            }
        } catch (Exception e) {
            return new BaseResponse<>("消息发送失败");
        }
        return new BaseResponse<>("消息发送成功");
    }

    /**
     * 延时消息发送
     * 对于一些，不需要返回的场景，比如日志记录
     *
     * @param text
     * @return
     */
    @RequestMapping("ScheduledProducerMessage")
    public BaseResponse<String> ScheduledProducerMessage(String text) {
        try {
            for (int i = 1; i <= 100; i++) {
                String sendText = "No." + i + " value:" + text;
                Message message = new Message(RocketMQConfig.TEST_TOPIC, "ScheduledMessage", sendText.getBytes());
                // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
                message.setDelayTimeLevel(3);
                SendResult res = defaultProducer.getMqProducer().send(message);
                System.out.println(res);
            }
        } catch (Exception e) {
            return new BaseResponse<>("消息发送失败");
        }
        return new BaseResponse<>("消息发送成功");
    }

    /**
     * 发送同步，顺序消息
     * 经典案例：发送订单消息，有三种类型，创建订单、支付订单、订单发货
     * 需要保证一个订单的三个消息按照顺序被消费
     * 在发送消息的时候对于同一个订单的三个消息应该被存放到统一队列中，才能在消费者那边做到顺序消费
     *
     * @return
     */
    @RequestMapping("syncProducerMessageOrder")
    public BaseResponse<String> syncProducerMessageOrder(String text) {
        // 三种tag用于过滤
        String[] tags = new String[]{"createTag", "payTag", "sendTag"};
        try {
            // 发100笔订单消息
            for (int orderId = 1; orderId <= 100; orderId++) {
                for (int j = 0; j < tags.length; j++) {
                    // 顺序消息的发送只要保证某一次消息发送都在同一个队列中，这是做到顺序消费的前提
                    String sendText = "OrderId：" + orderId + " type:" + j +" tag:" + tags[j];
                    Message message = new Message(RocketMQConfig.ORDER_TOPIC, tags[j], sendText.getBytes());
                    // 通过send方法中传入的MessageQueueSelector来实现选择对应的队列
                    // args一般是唯一id 用send的第三个参数传入（这里指的i）
                    // 这里是通过 订单id 对队列数取余来保证属于每一个订单的三条消息会被发送到同一个队列中
                    SendResult result = orderProducerA.getMqProducer().send(message, (mqs, msg, arg) -> {
                        int queueNum = (Integer) arg % mqs.size();
                        return mqs.get(queueNum);
                    }, orderId);
                    System.out.println(result);
                }
            }
        } catch (Exception e) {
            return new BaseResponse<>("消息发送失败");
        }
        return new BaseResponse<>("消息发送成功");
    }

    /**
     * 广播消息发送
     *
     * @param text
     * @return
     */
    @RequestMapping("broadcastProducerMessage")
    public BaseResponse<String> broadcastProducerMessage(String text) {
        try {
            for (int i = 1; i <= 1; i++) {
                String sendText = "No." + i + " value:" + text;
                Message message = new Message(RocketMQConfig.BROADCAST_TOPIC, "broadcastMessage", sendText.getBytes());
                SendResult res = defaultProducer.getMqProducer().send(message);
                System.out.println(res);
            }
        } catch (Exception e) {
            return new BaseResponse<>("消息发送失败");
        }
        return new BaseResponse<>("消息发送成功");
    }

}
