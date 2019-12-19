package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Orchestrator {

    private static Logger logger = LoggerFactory.getLogger(Orchestrator.class);
    private static Config configuration;
    private ActiveMQConnectionFactory primaryConnectionFactory;
    private ActiveMQConnectionFactory standbyConnectionFactory;
    private static Connection primaryConnection;
    private static Connection standbyConnection;
    private Session primarySession;
    private Session standbySession;
    private MessageSet mset = MessageSet.getInstance();
    private static HashMap<String, ExecutorService> queueThreads = new HashMap<>();


    public Orchestrator(Config configuration) {
        this.configuration = configuration;
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
        executorService = Executors.newFixedThreadPool(numThreads);
        queueThreads.put(queue, executorService);
        for (int i = 0; i < numThreads; i++) {
            Session standbySession = null;
            try {
                standbySession = standbyConnection.createSession(true, Session.SESSION_TRANSACTED);
            } catch (JMSException e) {
                e.printStackTrace();
            }
            QueueReader qr = new QueueReader(queue + ".DR", standbySession);
            qr.setRetryTimeout(configuration.getRetryTimeout());
            executorService.execute(qr);
        }
    }

    public static void stopReaders(String queue) {
        ExecutorService executorService = queueThreads.get(queue);
        if (executorService != null) {

            executorService.shutdown();
        }
    }

    public void  start() {
        logger.info("Starting primary processor...");
        try {

            primaryConnection = primaryConnectionFactory.createConnection();
            primaryConnection.start();

            standbyConnection = standbyConnectionFactory.createConnection();
            standbyConnection.start();

            primarySession = primaryConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Iterator<String> iterator = configuration.getDrQueues().iterator();

            while(iterator.hasNext()) {
                String queue = iterator.next();

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
            e.printStackTrace();
        }
    }
}
