package com.example;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import javax.jms.*;
import java.util.HashMap;

public class ConsumedMessageListener implements MessageListener, ExceptionListener {

    private static Logger logger = LoggerFactory.getLogger(ConsumedMessageListener.class);
    private DynamoDbClient client;
    private String tableName;
    private AttributeValue queueSync;

    public ConsumedMessageListener(String ddbTableName) {
        client = DynamoDbClient.builder().region(Region.US_EAST_1).build();
        this.tableName = ddbTableName;
        queueSync = AttributeValue.builder().bool(false).build();
    }

    @Override
    public void onException(JMSException e) {
        logger.error(e.toString());
    }

    @Override
    public void onMessage(Message message) {
        HashMap<String, AttributeValue> fields = new HashMap<String, AttributeValue>();
        try {
            ActiveMQMessage amqMessage = (ActiveMQMessage) message;
            ActiveMQTextMessage amqTextMessage = (ActiveMQTextMessage)amqMessage.getDataStructure();
            logger.debug("messageDump " + amqMessage.toString());
            logger.debug("messageDump " + amqTextMessage.toString());
            String destName = amqTextMessage.getDestination().getPhysicalName();
            AttributeValue dest = AttributeValue.builder().
                    s(destName).build();
            AttributeValue msgId = AttributeValue.builder().
                                    s(amqTextMessage.getJMSCorrelationID()).build();
            AttributeValue brokerId = AttributeValue.builder().
                                    s(amqMessage.getStringProperty("originBrokerId")).build();
            AttributeValue producerId = AttributeValue.builder().
                                    s(amqTextMessage.getProducerId().toString()).build();
            AttributeValue timestamp = AttributeValue.builder().
                                    s(String.valueOf(amqTextMessage.getTimestamp())).build();

            fields.put("messageId", msgId);
            fields.put("queueName", dest);
            fields.put("brokerId", brokerId);
            fields.put("producerId", producerId);
            fields.put("timestamp", timestamp);
            fields.put("queueSync", queueSync);
            logger.info("Put field values " + fields.values());
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(this.tableName)
                    .item(fields)
                    .build();
            client.putItem(request);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
