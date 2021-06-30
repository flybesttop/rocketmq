package com.flybesttop.rocketmq.producer;

import com.flybesttop.rocketmq.config.RocketMQConfig;
import com.flybesttop.rocketmq.listener.TransactionListenerImpl;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.*;

/**
 * @description 事务消息
 * @author flybesttop
 */
@Component
public class TransactionProducer {

    private String producerGroup = RocketMQConfig.TRANSACTION_PRODUCER_GROUP;

    @Value(value = "${RocketMQ.nameServerAddr}")
    private String nameServerAddr;

    /**
     * 需要一个默认的消息生产者
     */
    private TransactionMQProducer mqProducer;

    public DefaultMQProducer getMqProducer() {
        return mqProducer;
    }

    /**
     * 在依赖注入之后自动调用初始化
     */
    @PostConstruct
    public void initSyncProducer(){

        //定义一个线程池 来提供broker回调查询状态
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        mqProducer=new TransactionMQProducer(producerGroup);
        mqProducer.setNamesrvAddr(nameServerAddr);
        mqProducer.setVipChannelEnabled(false);
        //事务相关
        mqProducer.setExecutorService(executorService);
        TransactionListener transactionListener = new TransactionListenerImpl();
        mqProducer.setTransactionListener(transactionListener);
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
