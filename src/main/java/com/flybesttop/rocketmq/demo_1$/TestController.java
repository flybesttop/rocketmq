package com.flybesttop.rocketmq.demo_1$;

import com.flybesttop.rocketmq.dto.BaseResponse;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author flybesttop
 * @description 测试
 */
@RestController
@RequestMapping("test")
public class TestController {

    @Autowired
    private TestProducer testProducer;

    private final static String TOPIC = "test_topic";

    @RequestMapping("testProducer")
    public BaseResponse<String> callBack(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(TOPIC, "testTag", text.getBytes());
        SendResult res = testProducer.getMqProducer().send(message);
        System.out.println(res);
        return new BaseResponse<>("消息发送成功");
    }

}
