package com.example;

import net.spy.memcached.internal.OperationFuture;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class ConsumedMessageListener implements MessageListener, ExceptionListener {

    private static Logger logger = LoggerFactory.getLogger(ConsumedMessageListener.class);
    private static String blank = " ";

    public ConsumedMessageListener() {
    }

    @Override
    public void onException(JMSException e) {
        logger.error(e.toString());
    }

    @Override
    public void onMessage(Message message) {
        try {
            ActiveMQMessage amqMessage = (ActiveMQMessage) message;
            ActiveMQTextMessage amqTextMessage = (ActiveMQTextMessage)amqMessage.getDataStructure();
            String msgId = amqTextMessage.getJMSCorrelationID();
            logger.info("msgId {}", msgId);
            final OperationFuture<Boolean> set = MessageSet.memcachedClient.set(msgId, 0, blank);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
