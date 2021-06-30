package com.flybesttop.rocketmq.listener;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;

/**
 * 事务消息监听器
 * @author mtx
 */
public class TransactionListenerImpl implements TransactionListener {
    /**
     * 运行本地事务，上传 commit 或者 rollback
     *
     * @param arg
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        String msgBody;
        //执行本地业务的时候，再插入一条数据到事务表中，供checkLocalTransaction进行check使用，避免doBusinessCommit业务成功，但是未返回Commit
        try {
            msgBody = new String(msg.getBody(), "utf-8");
            doBusinessCommit(msg.getKeys(),msgBody);
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return LocalTransactionState.ROLLBACK_MESSAGE;

        }
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        Boolean result=checkBusinessStatus(msg.getKeys());
        if(result){
            return LocalTransactionState.COMMIT_MESSAGE;
        }else{
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

    }

    public static void doBusinessCommit(String messageKey,String msgbody){
        // 可以在这里执行对应事务 TODO
//        int i = 1/0;
        System.out.println("do something in DataBase");
        System.out.println("insert 事务消息到本地消息表中，消息执行成功，messageKey为："+messageKey);
    }

    public static Boolean checkBusinessStatus(String messageKey){
        if(true){
            System.out.println("查询数据库 messageKey为"+messageKey+"的消息已经消费成功了，可以提交消息");
            return true;
        }else{
            System.out.println("查询数据库 messageKey为"+messageKey+"的消息不存在或者未消费成功了，可以回滚消息");
            return false;
        }
    }

}
