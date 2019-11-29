package com.example;

import com.amazonaws.regions.Regions;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;

import javax.jms.JMSException;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StandbyProcessor {
    private Config configuration;
    private ActiveMQConnectionFactory connectionFactory;
    private ActiveMQConnection connection;
    private List<ActiveMQConnection> connections;
    private List<Session> sessions;
    ExecutorService executorService;

    public StandbyProcessor(Config configuration) {
        this.configuration = configuration;
        connectionFactory = new ActiveMQConnectionFactory(configuration.getBrokerURL());
        connectionFactory.setUserName(configuration.getUserName());
        connectionFactory.setPassword(configuration.getPassword());
        executorService = Executors.newFixedThreadPool(configuration.getDrQueues().size());
    }

    public void  start() {
        connections = new ArrayList<ActiveMQConnection>();
        sessions = new ArrayList<Session>();
        MessageSyncStream stream = new MessageSyncStream("arn:aws:dynamodb:us-west-2:668829368920:table/dr-message-sync/stream/2019-11-27T19:39:24.828", Regions.US_WEST_2);
        stream.start();
        try {
            Iterator<String> iterator = configuration.getDrQueues().iterator();
            while(iterator.hasNext()) {
                String queue = iterator.next();
                connection = (ActiveMQConnection) connectionFactory.createConnection();
                connections.add(connection);
                connection.start();
//                RedeliveryPolicy policy = connection.getRedeliveryPolicy();
//                policy.setInitialRedeliveryDelay(500);
//                policy.setBackOffMultiplier(2);
//                policy.setUseExponentialBackOff(true);
//                policy.setMaximumRedeliveries(6);
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                sessions.add(session);
                QueueReader qr = new QueueReader(queue, session, configuration.getDdbTable());
                executorService.execute(qr);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
