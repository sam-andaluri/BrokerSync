package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Iterator;

public class PrimaryProcessor {
    private static Logger logger = LoggerFactory.getLogger(PrimaryProcessor.class);
    private Config configuration;
    private ActiveMQConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;


    public PrimaryProcessor(Config configuration) {
        this.configuration = configuration;
        connectionFactory = new ActiveMQConnectionFactory(configuration.getBrokerURL());
        connectionFactory.setUserName(configuration.getUserName());
        connectionFactory.setPassword(configuration.getPassword());
    }

    public void  start() {
        logger.info("Starting primary processor...");
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Iterator<String> iterator = configuration.getDrQueues().iterator();
            while(iterator.hasNext()) {
                String topic = "ActiveMQ.Advisory.MessageConsumed.Queue." + iterator.next();
                Destination destination = session.createTopic(topic);
                MessageConsumer consumer = session.createConsumer(destination);
                consumer.setMessageListener(new ConsumedMessageListener(configuration.getDdbTable()));
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
