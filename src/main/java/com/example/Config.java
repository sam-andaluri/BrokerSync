package com.example;

import java.util.List;

public class Config {

    private String brokerURL;
    private String userName;
    private String password;
    private String ddbTable;
    private List<String> drQueues;

    public String getBrokerURL() {
        return brokerURL;
    }

    public void setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDdbTable() {
        return ddbTable;
    }

    public void setDdbTable(String ddbTable) {
        this.ddbTable = ddbTable;
    }

    public List<String> getDrQueues() {
        return drQueues;
    }

    public void setDrQueues(List<String> drQueues) {
        this.drQueues = drQueues;
    }
}
