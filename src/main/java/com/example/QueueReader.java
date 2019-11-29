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

    private String tableName;

    public QueueReader(String queueName, Session session, String tableName) {
        this.queueName = queueName;
        this.session = session;
        this.tableName = tableName;
    }

    public boolean transact(String dest, String msgId) {
        if (MessageSet.getInstance(this.tableName).mark(dest, msgId)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void run() {
        try {
            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);
            Thread.sleep(5000);
            while (true) {
                ActiveMQTextMessage message = (ActiveMQTextMessage)consumer.receive(30000);
                if (message != null) {
                    String qName = message.getDestination().getPhysicalName();
                    String destName = qName.substring(0, queueName.length() - 3);
                    String correlationId = message.getJMSCorrelationID();
                    logger.info("QueueReader: processing " + destName + ",msgId:" + correlationId);
                    if (transact(destName, correlationId)) {
                        session.commit();
                    } else {
                        Thread.sleep(5000);
                        if (transact(destName, correlationId)) {
                            session.commit();
                        } else {
                            session.rollback();
                        }
                    }
                } else {
                    logger.info("QueueReader: No messages to read...");
                }
             }
        } catch (JMSException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
