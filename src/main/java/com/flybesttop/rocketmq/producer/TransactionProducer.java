package com.flybesttop.rocketmq.producer;

import com.flybesttop.rocketmq.config.RocketMQConfig;
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
        mqProducer.setTransactionListener(new TransactionListenerImpl());
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

    /**
     * 事务监听
     */
    class TransactionListenerImpl implements TransactionListener {
        /**
         * 第一次判断是否提交或回滚
         *
         * @param message
         * @param arg
         * @return
         */
        @Override
        public LocalTransactionState executeLocalTransaction(Message message, Object arg){

            //message就是那个半发送的消息 arg是在transcationProducter.send(Message,Object)时的另外一个携带参数）

            //执行本地事务或调用其他为服务

            if(true){
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            if(true){
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }

            //如果在检查事务时数据库出现宕机可以让broker过一段时间回查 和return null 效果相同
            return LocalTransactionState.UNKNOW;
        }

        /**
         * 返回UNKOWN时回查！
         * @param messageExt
         * @return
         */
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
            //只去返回commit或者rollback
            return LocalTransactionState.COMMIT_MESSAGE;
        }

    }

}
