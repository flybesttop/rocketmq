package com.flybesttop.rocketmq.demo_1$;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.stereotype.Component;

/**
 * @author flybesttop
 * @description 测试消息生产者
 */
@Component
public class TestProducer {

    private String producerGroup = "test_group";

    private String nameServerAddr = "42.192.197.225:9876";

    /**
     * 需要一个默认的消息生产者
     */
    private DefaultMQProducer mqProducer;

    public TestProducer() {
        mqProducer = new DefaultMQProducer();
        mqProducer.setNamesrvAddr(nameServerAddr);
        //初始化
        start();
    }

    ;

    public DefaultMQProducer getMqProducer() {
        return mqProducer;
    }


    /**
     * 启动
     */
    public void start() {
        try {
            mqProducer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }


    /**
     * 关闭
     */
    public void shutdown() {
        mqProducer.shutdown();
    }

}
