package com.wez.rocketmq.transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消息消费者
 * @Author wez
 * @Date 2021/4/4
 */
public class MessageConsumer {

    private static final String CONSUMER_GROUP = "TransactionConsumerGroup";
    private static final String NAME_SERVER_ADDRESS = "localhost:9876";
    private static final String TOPIC_NAME = "TransactionTopicTest";

    public static void main(String[] args) throws InterruptedException, MQClientException {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

        // 设置NameServer的地址
        consumer.setNamesrvAddr(NAME_SERVER_ADDRESS);

        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe(TOPIC_NAME, "*");
        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("%s Receive New Messages: MessageId=%s, TransactionId=%s, Tags=%s %n",
                            Thread.currentThread().getName(), msg.getMsgId(), msg.getTransactionId(), msg.getTags());

                    // 官网说明：事务性消息可能不止一次被检查或消费。
                    // 注意：所以在实际业务处理时，需要增加幂等性判断，防止重复消费消息
                }

                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

}
