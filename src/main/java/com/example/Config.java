package com.example;

import java.util.List;

public class Config {

    private String primaryBrokerURL;
    private String primaryBrokerUsername;
    private String primaryBrokerPassword;
    private String standbyBrokerURL;
    private String standbyBrokerUsername;
    private String standbyBrokerPassword;
    private List<String> drQueues;
    private int threadsPerQueue;
    private int retryTimeout;

    public void setPrimaryBrokerURL(String primaryBrokerURL) {
        this.primaryBrokerURL = primaryBrokerURL;
    }

    public void setPrimaryBrokerUsername(String primaryBrokerUsername) {
        this.primaryBrokerUsername = primaryBrokerUsername;
    }

    public void setPrimaryBrokerPassword(String primaryBrokerPassword) {
        this.primaryBrokerPassword = primaryBrokerPassword;
    }

    public void setStandbyBrokerURL(String standbyBrokerURL) {
        this.standbyBrokerURL = standbyBrokerURL;
    }

    public void setStandbyBrokerUsername(String standbyBrokerUsername) {
        this.standbyBrokerUsername = standbyBrokerUsername;
    }

    public void setStandbyBrokerPassword(String standbyBrokerPassword) {
        this.standbyBrokerPassword = standbyBrokerPassword;
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

    public String getPrimaryBrokerURL() {
        return primaryBrokerURL;
    }

    public String getPrimaryBrokerUsername() {
        return primaryBrokerUsername;
    }

    public String getPrimaryBrokerPassword() {
        return primaryBrokerPassword;
    }

    public String getStandbyBrokerURL() {
        return standbyBrokerURL;
    }

    public String getStandbyBrokerUsername() {
        return standbyBrokerUsername;
    }

    public String getStandbyBrokerPassword() {
        return standbyBrokerPassword;
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

}
