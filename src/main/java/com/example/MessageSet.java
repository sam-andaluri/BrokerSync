package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class MessageSet {

    private static Logger logger = LoggerFactory.getLogger(MessageSet.class);

    private static ConcurrentHashMap<String, Boolean> messageSet = new ConcurrentHashMap<>();

    private static HashMap<String, AttributeValue> fields = new HashMap<>();
    String tableName;
    private DynamoDbClient client;

    public static HashSet<String> consumedMsgs = new HashSet<>();
    public static HashSet<String> readMsgs = new HashSet<>();

    private MessageSet(String tableName) {
        this.tableName = tableName;
        client = DynamoDbClient.builder().region(Region.US_WEST_2).build();
    }

    public static MessageSet getInstance(String tableName) {
        return new MessageSet(tableName);
    }

    public ConcurrentHashMap<String, Boolean> getMap() {
        final ConcurrentHashMap<String, Boolean> messageSet = MessageSet.messageSet;
        return messageSet;
    }

    public void put(String destination, String messageId) {
        String key = messageId.concat(destination);
        logger.info("put key " + key);
        messageSet.put(key, false);
    }

    private boolean deleteRecord(String queueName, String messageId) {
        boolean success = true;
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
            if (result != null) {
                if (result.attributes().size() > 0 && result.attributes().get("brokerId").s()==null) {
                    success = false;
                }
            } else {
                success = false;
            }
            logger.info("Result " + result.toString());
        } catch (Exception e) {
            success = false;
            e.printStackTrace();
        }
        return success;
    }

    private boolean updateRecord(String queueName, String messageId) {
        boolean success = true;
        AttributeValue dest = AttributeValue.builder().
                s(queueName).build();
        AttributeValue msgId = AttributeValue.builder().
                s(messageId).build();
        AttributeValue sync = AttributeValue.builder().
                bool(true).build();
        fields.put("queueName", dest);
        fields.put("messageId", msgId);
        fields.put("queueSync", sync);
        logger.info("UpdateRecord req for {}", fields.values());
        try {
            ReturnValue returnValues = ReturnValue.ALL_OLD;
            UpdateItemRequest updateReq = UpdateItemRequest.builder()
                    .tableName(this.tableName)
                    .returnValues(returnValues)
                    .key(fields)
                    .build();
            UpdateItemResponse result = client.updateItem(updateReq);
            if (result != null) {
                if (result.attributes().get("brokerId").s()==null) {
                    success = false;
                }
            } else {
                success = false;
            }
            logger.info("Result " + result.toString());
        } catch (Exception e) {
            success = false;
            e.printStackTrace();
        }
        return success;
    }

    public boolean mark(String destination, String messageId) {
        boolean success = true;
        if (!deleteRecord(destination, messageId)) {
            success = false;
        }
        return success;
    }
}
