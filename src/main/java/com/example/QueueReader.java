package com.example;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.*;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.logging.Logger;

public class QueueReader implements Runnable {
    private static Logger logger = Logger.getLogger(QueueReader.class.getName());
    private String queueName;
    private Session session;
    private int retryTimeout = 1000;
    private String cwTag = "TAG";
    private static MessageSet mset = MessageSet.getInstance();
    private String hostname;
    private String machineThreadId;
    private static Dimension dimension;
    private static AmazonCloudWatch cloudWatch;
    private static double consumerTPS = 0;
    private static DoubleAdder consumerTPSCounter = new DoubleAdder();
    private static boolean gotStats = false;

    private static class CWTimerTask extends TimerTask {

        @Override
        public void run() {
            if (gotStats) {
                consumerTPS = consumerTPSCounter.sum();
                MetricDatum datumConsumerTPS = new MetricDatum()
                        .withMetricName("NumMessages")
                        .withUnit(StandardUnit.CountSecond)
                        .withValue(consumerTPS)
                        .withDimensions(dimension);
                PutMetricDataRequest request = new PutMetricDataRequest()
                        .withNamespace("JMS/app")
                        .withMetricData(datumConsumerTPS);
                PutMetricDataResult result = cloudWatch.putMetricData(request);
                consumerTPSCounter.reset();
                logger.info(result.getSdkResponseMetadata().toString());
            }
        }
    }

    public QueueReader(String queueName, Session session) {
        this.queueName = queueName;
        this.session = session;
    }

    public void multiThreadedRunner() {
        MessageSet mset = MessageSet.getInstance();
        try {
            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);
            ActiveMQTextMessage message = (ActiveMQTextMessage)consumer.receive(retryTimeout);
            Thread.sleep(retryTimeout);
            while (!Thread.currentThread().isInterrupted()) {
                if (message != null) {
                    if(!gotStats) gotStats = true;
                    String correlationId = message.getJMSCorrelationID();
                    if (mset.memcachedClient.get(correlationId) != null) {
                        logger.info("commit " + correlationId);
                        session.commit();
                    } else {
                        session.rollback();
                        Thread.sleep(retryTimeout);
                    }
                } else {
                    logger.info("QueueReader: No messages to read...");
                    Thread.sleep(retryTimeout);
                }
                message = (ActiveMQTextMessage)consumer.receive(retryTimeout);
                consumerTPSCounter.add(1.0);
            }
        } catch (JMSException e) {
            logger.severe("JMS Error " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void setRetryTimeout(int retryTimeout) {
        this.retryTimeout = retryTimeout;
    }

    public void setCwTag(String cwTag) {
        this.cwTag = cwTag;
    }

    @Override
    public void run() {
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            machineThreadId = hostname + '-' + Thread.currentThread().currentThread().getId();
            dimension = new Dimension().withName("BrokerSync").withValue(machineThreadId + "-" + cwTag);
            cloudWatch = AmazonCloudWatchClientBuilder.defaultClient();
        } catch (UnknownHostException e) {
            logger.severe("Error in run : " + e.getMessage());
        }
        Timer cwTimer = new Timer();
        TimerTask sendMetrics = new CWTimerTask();
        cwTimer.schedule(sendMetrics, 0, 1000);
        multiThreadedRunner();
    }
}
