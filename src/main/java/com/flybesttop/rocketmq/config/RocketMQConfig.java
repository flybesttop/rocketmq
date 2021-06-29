package com.flybesttop.rocketmq.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * RocketMQ配置
 * @author mtx
 */
@Configuration
public interface RocketMQConfig {

    String TEST_TOPIC = "test_topic";

    String TEST_PRODUCER_GROUP = "test_producer_group";

    String TEST_CONSUMER_GROUP = "test_consumer_group";
}
