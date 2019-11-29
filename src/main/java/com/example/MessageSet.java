package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessageSet {

    private static Logger logger = LoggerFactory.getLogger(MessageSet.class);
    private static ConcurrentHashMap<String, Boolean> messageSet = new ConcurrentHashMap<String, Boolean>();
    private static HashMap<String, AttributeValue> fields = new HashMap<String, AttributeValue>();
    String tableName;
    private DynamoDbClient client;

    private MessageSet(String tableName) {
        this.tableName = tableName;
        client = DynamoDbClient.builder().region(Region.US_WEST_2).build();
    }

    public static MessageSet getInstance(String tableName) {
        return new MessageSet(tableName);
    }

    public void put(String destination, String messageId) {
        String key = messageId.concat(destination);
        logger.info("put key " + key);
//        logger.info("messageSet size before " + messageSet.size());
        messageSet.put(key, false);
//        logger.info("messageSet size before " + messageSet.size());
    }

    private boolean deleteRecord(String queueName, String messageId) {
        boolean success = true;
        AttributeValue dest = AttributeValue.builder().
                s(queueName).build();
        AttributeValue msgId = AttributeValue.builder().
                s(messageId).build();
        fields.put("queueName", dest);
        fields.put("messageId", msgId);
        logger.info("DeleteRecord req for " + fields.values());
        try {
            ReturnValue returnValues = ReturnValue.ALL_OLD;
            DeleteItemRequest deleteReq = DeleteItemRequest.builder()
                    .tableName(this.tableName)
                    .returnValues(returnValues)
                    .key(fields)
                    .build();
            DeleteItemResponse result = client.deleteItem(deleteReq);
            if (result.attributes().get("brokerId").s()==null) {
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
        String key = messageId.concat(destination);
        logger.info("mark key " + messageId.concat(destination));
        boolean success = true;
//        logger.info("messageSet size " + messageSet.size());
//        Iterator it = messageSet.entrySet().iterator();
//        while(it.hasNext()) {
//            logger.info("hash contains " + it.next().toString());
//        }
        if (messageSet.containsKey(key)) {
            logger.info("containsKey " + key);
            if (deleteRecord(destination, messageId)) {
                messageSet.remove(key);
            }
        } else {
            success = false;
        }
        return success;
    }
}
