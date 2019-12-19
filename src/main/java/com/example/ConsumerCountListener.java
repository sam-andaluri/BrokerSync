package com.example;

import org.apache.activemq.command.ActiveMQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

public class ConsumerCountListener implements MessageListener, ExceptionListener {

    private static Logger logger = LoggerFactory.getLogger(ConsumerCountListener.class);

    @Override
    public void onMessage(Message message) {

        ActiveMQMessage amqMessage = (ActiveMQMessage) message;

        try {
            String queueName = message.getJMSDestination().toString();
            int consumerCount = amqMessage.getIntProperty("consumerCount");
            MessageSet.memcachedClient.set(queueName, 0, consumerCount);
            logger.info("{} consumerCount = {}", queueName, consumerCount);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        logger.info(message.toString());
    }

    @Override
    public void onException(JMSException e) {

    }
}
