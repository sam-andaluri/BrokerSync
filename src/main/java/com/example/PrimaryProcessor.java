package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PrimaryProcessor {
    private static Logger logger = LoggerFactory.getLogger(PrimaryProcessor.class);
    private Config configuration;
    private ActiveMQConnectionFactory primaryConnectionFactory;
    private ActiveMQConnectionFactory standbyConnectionFactory;
    private Connection primaryConnection;
    private Connection standbyConnection;
    private Session primarySession;
    private Session standbySession;
    ExecutorService executorService;


    public PrimaryProcessor(Config configuration) {
        this.configuration = configuration;
        primaryConnectionFactory = new ActiveMQConnectionFactory(configuration.getPrimaryBrokerURL());
        primaryConnectionFactory.setUserName(configuration.getPrimaryBrokerUsername());
        primaryConnectionFactory.setPassword(configuration.getPrimaryBrokerPassword());

        standbyConnectionFactory = new ActiveMQConnectionFactory(configuration.getStandbyBrokerURL());
        standbyConnectionFactory.setUserName(configuration.getStandbyBrokerUsername());
        standbyConnectionFactory.setPassword(configuration.getStandbyBrokerPassword());

        executorService = Executors.newFixedThreadPool(configuration.getThreadsPerQueue());
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
                numConsumersDestConsumer.setMessageListener(new ConsumerCountListener());

                for (int i = 0; i < configuration.getThreadsPerQueue(); i++) {
                    Session standbySession = standbyConnection.createSession(true, Session.SESSION_TRANSACTED);
                    QueueReader qr = new QueueReader(queue + ".DR", standbySession);
                    qr.setRetryTimeout(configuration.getRetryTimeout());
                    executorService.execute(qr);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
