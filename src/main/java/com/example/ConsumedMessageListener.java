package com.example;

import net.spy.memcached.internal.OperationFuture;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;

import java.util.logging.Logger;

import javax.jms.*;

public class ConsumedMessageListener implements MessageListener, ExceptionListener {

    private static Logger logger = Logger.getLogger(ConsumedMessageListener.class.getName());
    private static String blank = " ";

    public ConsumedMessageListener() {
    }

    @Override
    public void onException(JMSException e) {
        logger.severe(e.toString());
    }

    @Override
    public void onMessage(Message message) {
        try {
            ActiveMQMessage amqMessage = (ActiveMQMessage) message;
            ActiveMQTextMessage amqTextMessage = (ActiveMQTextMessage)amqMessage.getDataStructure();
            String msgId = amqTextMessage.getJMSCorrelationID();
            logger.info("add msgId to cache : " + msgId);
            final OperationFuture<Boolean> set = MessageSet.memcachedClient.set(msgId, 0, blank);
        } catch (Exception e) {
            logger.severe( "Consumed onMessage error : " + e.getMessage());
        }
    }
}
