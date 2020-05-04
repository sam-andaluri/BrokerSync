package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;
import java.util.logging.Logger;

import javax.jms.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Orchestrator {

    private static Logger logger = Logger.getLogger(Orchestrator.class.getName());
    private static Config configuration;
    private ActiveMQConnectionFactory primaryConnectionFactory;
    private ActiveMQConnectionFactory standbyConnectionFactory;
    private static Connection primaryConnection;
    private static Connection standbyConnection;
    private Session primarySession;
    private Session standbySession;
    private MessageSet mset = MessageSet.getInstance();
    private static HashMap<String, ExecutorService> queueThreads = new HashMap<>();
    private static HashMap<String, ArrayList<Session>> queueSessions = new HashMap<>();


    public Orchestrator(Config configuration) {
        this.configuration = configuration;

        configuration.dump();

        primaryConnectionFactory = new ActiveMQConnectionFactory(configuration.getPrimaryBrokerURL());
        primaryConnectionFactory.setUserName(configuration.getPrimaryBrokerUsername());
        primaryConnectionFactory.setPassword(configuration.getPrimaryBrokerPassword());

        standbyConnectionFactory = new ActiveMQConnectionFactory(configuration.getStandbyBrokerURL());
        standbyConnectionFactory.setUserName(configuration.getStandbyBrokerUsername());
        standbyConnectionFactory.setPassword(configuration.getStandbyBrokerPassword());
    }

    public static void startReaders(String queue, int consumerCount) {
        ExecutorService executorService;
        int numThreads = configuration.getThreadsPerQueue() * consumerCount;
        logger.info("NumThreads " + numThreads + " ThreadsPerQueue " + configuration.getThreadsPerQueue() + " consumerCount " + consumerCount);
        executorService = Executors.newFixedThreadPool(numThreads);
        queueThreads.put(queue, executorService);
        ArrayList sessions = new ArrayList();
        for (int i = 0; i < numThreads; i++) {
            Session standbySession = null;
            try {
                standbySession = standbyConnection.createSession(true, Session.SESSION_TRANSACTED);
                sessions.add(standbySession);
            } catch (JMSException e) {
                logger.severe( "Unable to connect to standby : " + e.getMessage());
            }
            QueueReader qr = new QueueReader(queue, standbySession);
            qr.setRetryTimeout(configuration.getRetryTimeout());
            qr.setCwTag(configuration.getCWTag());
            executorService.execute(qr);
        }
        queueSessions.put(queue, sessions);
    }

    public static void stopReaders(String queue) {
        Iterator<Session> it = queueSessions.get(queue).iterator();
        while (it.hasNext()) {
            Session s = it.next();
            try {
                s.close();
            } catch (JMSException e) {
                logger.severe( "stop error : " + e.getMessage());
            }
        }
        ExecutorService executorService = queueThreads.get(queue);
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public void  start() {
        logger.info( "Starting orchestrator...");
        try {

            primaryConnection = primaryConnectionFactory.createConnection();
            primaryConnection.start();

            standbyConnection = standbyConnectionFactory.createConnection();
            standbyConnection.start();

            primarySession = primaryConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Iterator<String> iterator = configuration.getDrQueues().iterator();

            logger.info( "Number of DR Queues : " + configuration.getDrQueues().size());

            while(iterator.hasNext()) {

                String queue = iterator.next();
                logger.info( "Starting DR queue listeners : " + queue);

                String consumedTopic = "ActiveMQ.Advisory.MessageConsumed.Queue." + queue;
                Destination consumedDest = primarySession.createTopic(consumedTopic);
                MessageConsumer consumedDestConsumer = primarySession.createConsumer(consumedDest);
                consumedDestConsumer.setMessageListener(new ConsumedMessageListener());

                String numConsumersTopic = "ActiveMQ.Advisory.Consumer.Queue." + queue;
                Destination numConsumersDest = primarySession.createTopic(numConsumersTopic);
                MessageConsumer numConsumersDestConsumer = primarySession.createConsumer(numConsumersDest);
                numConsumersDestConsumer.setMessageListener(new ConsumerCountListener(queue));
            }
        } catch (Exception e) {
            logger.severe("Error : " + e.getMessage());
        }
    }
}
