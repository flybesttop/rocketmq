package com.flybesttop.rocketmq.listener;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @description 自定义监听
 * @author flybesttop
 */
public class MyOrderMsgListener implements MessageListenerOrderly {

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
        // 设置自动提交
        context.setAutoCommit(true);
        try {
            //模拟业务处理消息的时间
            try {
                Thread.sleep(new Random().nextInt(1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(list.get(0).getBody()));
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
    }
}
