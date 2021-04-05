package com.wez.rocketmq.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事务消息 - 事务监听接口实现
 * @Author wez
 * @Date 2021/4/4
 */
public class TransactionListenerImpl implements TransactionListener {

    private AtomicInteger transactionIndex = new AtomicInteger(0);

    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

    /**
     * 2. Half Msg Send OK
     * 执行本地事务
     * @param msg
     * @param arg
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.printf("2. Half Msg Send OK. TransactionId=%s, Tags=%s\n", msg.getTransactionId(), msg.getTags());

        // 3. Execute Location Transaction
        System.out.printf("3. Execute Location Transaction. TransactionId=%s, Tags=%s\n", msg.getTransactionId(), msg.getTags());

        LocalTransactionState state = null;
        String tags = msg.getTags();
        if ("TagA".equals(tags)) {
            state = LocalTransactionState.COMMIT_MESSAGE;
        } else if ("TagB".equals(tags)) {
            state = LocalTransactionState.ROLLBACK_MESSAGE;
        } else if ("TagC".equals(tags)) {
            state = LocalTransactionState.UNKNOW;
        } else {
            state = LocalTransactionState.UNKNOW;
        }

        // 4. Commit or RollBack or UNKNOW
        System.out.printf("4. Commit or RollBack or UNKNOW. TransactionId=%s, Tags=%s\n", msg.getTransactionId(), msg.getTags());
        return state;
    }

    /**
     * 5. Check back when the fourth step confirmation is not received.
     * 回查本地事务状态，处理执行本地事务时，返回LocalTransactionState.UNKNOW状态的消息
     * @param msg
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.printf("5. Check back when the fourth step confirmation is not received. MessageId=%s, TransactionId=%s\n",
                msg.getMsgId(), msg.getTransactionId());

        // 6. Check the state of Local Transaction.
        System.out.printf("6. Check the state of Local Transaction. MessageId=%s, TransactionId=%s, Tags=%s\n",
                msg.getMsgId(), msg.getTransactionId(), msg.getTags());

        // 执行回查业务逻辑
        LocalTransactionState state = null;
        String tags = msg.getTags();
        if ("TagC".equals(tags)) {
            state = LocalTransactionState.COMMIT_MESSAGE; // 提交
        } else if ("TagD".equals(tags)) {
            state = LocalTransactionState.ROLLBACK_MESSAGE; // 回滚
        } else if ("TagE".equals(tags)) {
            state = LocalTransactionState.UNKNOW; // 回查。则默认回查15次，如果15次回查还是无法得知事务状态，rocketmq默认回滚该消息。
        } else {
            state = LocalTransactionState.UNKNOW; // 回查。则默认回查15次，如果15次回查还是无法得知事务状态，rocketmq默认回滚该消息。
        }

        // 7. Commit or RollBack according to transaction status.
        System.out.printf("7. Commit or RollBack according to transaction status. MessageId=%s, TransactionId=%s, Tags=%s\n",
                msg.getMsgId(), msg.getTransactionId(), msg.getTags());
        return state;
    }
}
