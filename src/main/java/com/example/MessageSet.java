package com.example;

import net.spy.memcached.MemcachedClient;
import java.util.logging.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;

public class MessageSet {

    private static Logger logger = Logger.getLogger(MessageSet.class.getName());

    public static HashSet<String> readMsgs = new HashSet<>();
    public static MemcachedClient memcachedClient;

    private MessageSet() {
        try {
            memcachedClient = new MemcachedClient(new InetSocketAddress(Config.getInstance().getCacheConfigURL(),
                                                                        Config.getInstance().getCacheConfigPort()));
        } catch (IOException e) {
            logger.severe( "Unable to connect memcached : " + e.getMessage());
        }
    }

    public static MessageSet getInstance() {
        return new MessageSet();
    }
}
