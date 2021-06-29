package com.flybesttop.rocketmq.producer;

import com.flybesttop.rocketmq.config.RocketMQConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @description 广播
 * @author flybesttop
 */
@Component
public class BroadcastProducer {

    private String producerGroup = RocketMQConfig.BROADCAST_PRODUCER_GROUP;

    @Value(value = "${RocketMQ.nameServerAddr}")
    private String nameServerAddr;

    /**
     * 需要一个默认的消息生产者
     */
    private DefaultMQProducer mqProducer;

    public DefaultMQProducer getMqProducer() {
        return mqProducer;
    }

    /**
     * 在依赖注入之后自动调用初始化
     */
    @PostConstruct
    public void initSyncProducer(){
        mqProducer=new DefaultMQProducer(producerGroup);
        mqProducer.setNamesrvAddr(nameServerAddr);
        mqProducer.setVipChannelEnabled(false);
        try {
            mqProducer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void destroySyncProducer(){
        mqProducer.shutdown();
    }
}
