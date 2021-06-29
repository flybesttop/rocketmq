package com.flybesttop.rocketmq.rest;

import com.flybesttop.rocketmq.config.RocketMQConfig;
import com.flybesttop.rocketmq.service.ProducerService;
import com.flybesttop.rocketmq.dto.BaseResponse;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
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
    private ProducerService producerService;

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
                String sendText = "No." + i + " value:" + i;
                Message message = new Message(RocketMQConfig.TEST_TOPIC, "syncMessage", sendText.getBytes());
                SendResult res = producerService.getMqProducer().send(message);
                System.out.println(res);
            }
        } catch (Exception e) {
            return new BaseResponse<>("消息发送失败");
        }
        return new BaseResponse<>("消息发送成功");
    }

    /**
     * 发送同步，顺序消息
     *
     * @return
     */
    @RequestMapping("syncProducerMessageOrder")
    public BaseResponse<String> syncProducerMessageOrder(String text) {
        try {
            //发100条消息测试
            for (int i = 1; i <= 100; i++) {
                //顺序消息的发送只要保证某一次消息发送都在同一个队列中，这是做到顺序消费的前提
                String sendText = "No." + i + " value:" + i;
                Message message = new Message(RocketMQConfig.TEST_TOPIC, "syncMessageOrder", sendText.getBytes());
                //通过send方法中传入的MessageQueueSelector来实现选择对应的队列
                //args一般是唯一id 用send的第三个参数传入（这里指的i）
                SendResult result = producerService.getMqProducer().send(message, (mqs, msg, arg) -> {
                    int queueNum = 1;
                    return mqs.get(queueNum);
                }, i);
                System.out.println(result);
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
        DefaultMQProducer producer = producerService.getMqProducer();
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
                String sendText = "No." + i + " value:" + i;
                Message message = new Message(RocketMQConfig.TEST_TOPIC, "syncMessage", sendText.getBytes());
                producerService.getMqProducer().sendOneway(message);
            }
        } catch (Exception e) {
            return new BaseResponse<>("消息发送失败");
        }
        return new BaseResponse<>("消息发送成功");
    }

}
