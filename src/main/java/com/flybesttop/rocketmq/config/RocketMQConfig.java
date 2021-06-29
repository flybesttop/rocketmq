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

    String ORDER_TOPIC = "order_topic";

    String BROADCAST_TOPIC = "broadcast_topic";

    String TRANSACTION_PRODUCER_GROUP = "transaction_topic";

    String TEST_PRODUCER_GROUP = "test_producer_group";

    String ORDER_PRODUCER_GROUP = "order_producer_group";

    String BROADCAST_PRODUCER_GROUP = "broadcast_producer_group";

    String TEST_CONSUMER_GROUP = "test_consumer_group";

    String ORDER_CONSUMER_GROUP = "order_consumer_group";

    String BROADCAST_CONSUMER_GROUP = "broadcast_consumer_group";
}
