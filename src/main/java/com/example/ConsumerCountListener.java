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
    private String queueName;

    public ConsumerCountListener(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public void onMessage(Message message) {

        ActiveMQMessage amqMessage = (ActiveMQMessage) message;

        try {
            int consumerCount = amqMessage.getIntProperty("consumerCount");
            logger.info("{} consumerCount = {}", queueName, consumerCount);
            if (consumerCount > 0) {
                Orchestrator.startReaders(queueName, consumerCount);
                logger.info("Starting queue readers");
            } else {
                Orchestrator.stopReaders(queueName);
                logger.info("Stopping queue readers");
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        logger.info(message.toString());
    }

    @Override
    public void onException(JMSException e) {
        logger.error("Count OnMessage Error", e);
    }
}
