package com.example;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

public class QueueReader implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(QueueReader.class);
    private String queueName;
    private Session session;
    private String nondrQueue;
    private int retryTimeout = 1000;

    public void setRetryTimeout(int timeout) {
        retryTimeout = timeout;
    }

    public QueueReader(String queueName, Session session) {
        this.queueName = queueName;
        this.nondrQueue = queueName.substring(0, queueName.length() - 3);
        this.session = session;
    }

    @Override
    public void run() {
        MessageSet mset = MessageSet.getInstance();
        nondrQueue = queueName.substring(0, queueName.length() - 3);
        try {
            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);
            ActiveMQTextMessage message = (ActiveMQTextMessage)consumer.receive(retryTimeout);
            Thread.sleep(2000);
            while (true) {
                if (message != null) {
                    String correlationId = message.getJMSCorrelationID();
                    if (mset.memcachedClient.get(correlationId) != null) {
                        logger.info("commit {}", correlationId);
                        session.commit();
                    } else {
                        session.rollback();
                        Thread.sleep(1000);
                    }
                } else {
                    logger.info("QueueReader: No messages to read...");
                    Thread.sleep(retryTimeout);
                }
                message = (ActiveMQTextMessage)consumer.receive(retryTimeout);
            }
        } catch (JMSException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
