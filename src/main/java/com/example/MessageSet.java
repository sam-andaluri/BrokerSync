package com.example;

import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;

public class MessageSet {

    private static Logger logger = LoggerFactory.getLogger(MessageSet.class);

    public static HashSet<String> readMsgs = new HashSet<>();
    public static MemcachedClient memcachedClient;

    private MessageSet() {
        try {
            memcachedClient = new MemcachedClient(new InetSocketAddress("127.0.0.1", 11211));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static MessageSet getInstance() {
        return new MessageSet();
    }
}
