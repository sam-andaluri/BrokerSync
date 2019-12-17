package com.example;

import java.util.List;

public class Config {

    private String brokerURL;
    private String userName;
    private String password;
    private String ddbTable;
    private List<String> drQueues;
    private int threadsPerQueue;
    private int retryTimeout;

    public String getBrokerURL() {
        return brokerURL;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getDdbTable() {
        return ddbTable;
    }

    public List<String> getDrQueues() {
        return drQueues;
    }

    public int getThreadsPerQueue() {
        return threadsPerQueue;
    }

    public int getRetryTimeout() {
        return retryTimeout;
    }

    public void setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDdbTable(String ddbTable) {
        this.ddbTable = ddbTable;
    }

    public void setDrQueues(List<String> drQueues) {
        this.drQueues = drQueues;
    }

    public void setThreadsPerQueue(int threadsPerQueue) {
        this.threadsPerQueue = threadsPerQueue;
    }

    public void setRetryTimeout(int retryTimeout) {
        this.retryTimeout = retryTimeout;
    }
}
