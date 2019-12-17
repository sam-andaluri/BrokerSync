package com.example;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

import java.util.HashMap;

public class QueueReader implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(QueueReader.class);
    private String queueName;
    private Session session;
    private String tableName;
    private DynamoDbClient client;
    private int retryTimeout = 1000;
    private static HashMap<String, AttributeValue> fields = new HashMap<String, AttributeValue>();

    public void setRetryTimeout(int timeout) {
        retryTimeout = timeout;
    }

    public QueueReader(String queueName, Session session, String tableName) {
        this.queueName = queueName;
        this.session = session;
        this.tableName = tableName;
        client = DynamoDbClient.builder().region(Region.US_WEST_2).build();
    }

    private boolean deleteRecord(String queueName, String messageId) {
        boolean success = false;
        AttributeValue dest = AttributeValue.builder().
                s(queueName).build();
        AttributeValue msgId = AttributeValue.builder().
                s(messageId).build();
        fields.put("queueName", dest);
        fields.put("messageId", msgId);
        logger.info("DeleteRecord req for {} ",  fields.values());
        try {
            ReturnValue returnValues = ReturnValue.ALL_OLD;
            DeleteItemRequest deleteReq = DeleteItemRequest.builder()
                    .tableName(this.tableName)
                    .returnValues(returnValues)
                    .key(fields)
                    .build();
            DeleteItemResponse result = client.deleteItem(deleteReq);
            if (result != null && result.attributes().size() > 0 && result.attributes().get("brokerId")!=null) {
                success = true;
            }
            logger.info("Result {} {} {}", result.toString(), result.attributes().size(), success);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return success;
    }

    public boolean commitDb(String dest, String msgId) {
        if (deleteRecord(dest, msgId)) {
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
            ActiveMQTextMessage message = (ActiveMQTextMessage)consumer.receive(retryTimeout);
            while (true) {
                if (message != null) {
                    String qName = message.getDestination().getPhysicalName();
                    String destName = qName.substring(0, queueName.length() - 3);
                    String correlationId = message.getJMSCorrelationID();
                    logger.info("{} processing queue {} msgId {}", Thread.currentThread().getId(), destName, correlationId);
                    if (commitDb(destName, correlationId)) {
                        session.commit();
                        message = (ActiveMQTextMessage)consumer.receive(retryTimeout);
                    } else {
                        Thread.sleep(retryTimeout);
                    }
                } else {
                    logger.debug("QueueReader: No messages to read...");
                    Thread.sleep(retryTimeout);
                    message = (ActiveMQTextMessage)consumer.receive(retryTimeout);
                }
            }
        } catch (JMSException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
