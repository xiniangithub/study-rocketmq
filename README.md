# v1.0
- RocketMQ：事务消息。

![Image of Yaktocat](./doc/images/事务消息.png)

根据官网事务消息代码示例理解图示过程，将图示过程在代码中使用注释标注出来。

事务消息生产者：[TransactionProducer](./study-rocketmq-transaction/src/main/java/com/wez/rocketmq/transaction/TransactionProducer.java)

事务消息监听器实现：[TransactionListenerImpl](./study-rocketmq-transaction/src/main/java/com/wez/rocketmq/transaction/TransactionListenerImpl.java)

消息消费者：[MessageConsumer](./study-rocketmq-transaction/src/main/java/com/wez/rocketmq/transaction/MessageConsumer.java)
