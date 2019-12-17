package com.example;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.JMSException;
import javax.jms.Session;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StandbyProcessor {
    private Config configuration;
    private ActiveMQConnectionFactory connectionFactory;
    private ActiveMQConnection connection;
    ExecutorService executorService;

    public StandbyProcessor(Config configuration) {
        this.configuration = configuration;
        connectionFactory = new ActiveMQConnectionFactory(configuration.getBrokerURL());
        connectionFactory.setUserName(configuration.getUserName());
        connectionFactory.setPassword(configuration.getPassword());
        executorService = Executors.newFixedThreadPool(configuration.getDrQueues().size() * configuration.getThreadsPerQueue());
    }

    public void  start() {
        try {
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();
        Iterator<String> iterator = configuration.getDrQueues().iterator();
        while(iterator.hasNext()) {
            String queue = iterator.next();
            for (int i = 0; i < configuration.getThreadsPerQueue(); i++) {
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                QueueReader qr = new QueueReader(queue, session, configuration.getDdbTable());
                qr.setRetryTimeout(configuration.getRetryTimeout());
                executorService.execute(qr);
            }
        }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
