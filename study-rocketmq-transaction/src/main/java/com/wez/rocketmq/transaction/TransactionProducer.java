package com.wez.rocketmq.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * 事务消息 - 生产者
 * @Author wez
 * @Date 2021/4/4
 */
public class TransactionProducer {

    private static final String PRODUCER_GROUP = "TransactionProducerGroup";
    private static final String NAME_SERVER_ADDRESS = "localhost:9876";
    private static final String TOPIC_NAME = "TransactionTopicTest";

    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 事务生产者
        TransactionMQProducer producer = new TransactionMQProducer(PRODUCER_GROUP);
        // 设置NameServer的地址
        producer.setNamesrvAddr(NAME_SERVER_ADDRESS);
        // 线程池
        producer.setExecutorService(buildThreadPool());
        // 事务监听接口
        producer.setTransactionListener(new TransactionListenerImpl());
        // 启动Producer实例
        producer.start();

        /*
            示例说明
            模拟发送五个事务消息，分别是TagA、TagB、TagC、TagD、TagE。
            1）TagA消息直接提交；
            2）TagB消息直接回滚；
            3）TagC消息回查再提交；
            4）TagD消息回查再回滚；
            5）TagE消息一直回查，直到回查次数达到上限有RocketMQ回滚。
         */
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 5; i++) {
            try {
                // 1. Send: half Msg.
                Message msg = new Message(TOPIC_NAME, tags[i % tags.length], "KEY" + i, ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("1. Send: half Msg. MessageId=%s, TransactionId=%s\n", sendResult.getMsgId(), sendResult.getTransactionId());
                Thread.sleep(10);
            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }

        producer.shutdown();
    }

    /**
     * 创建线程池
     * @return
     */
    public static ExecutorService buildThreadPool() {
        return new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
    }

}
