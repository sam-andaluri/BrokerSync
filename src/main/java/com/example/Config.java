package com.example;

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Config {

    private String primaryBrokerURL;
    private String primaryBrokerUsername;
    private String primaryBrokerPassword;
    private String standbyBrokerURL;
    private String standbyBrokerUsername;
    private String standbyBrokerPassword;
    private String cacheConfigURL;
    private String cacheConfigPort;
    private List<String> drQueues = new ArrayList<String>();
    private int threadsPerQueue;
    private int retryTimeout;
    private String cwTag;

    public static Config getInstance() {
        return new Config();
    }

    private static Logger logger = Logger.getLogger(Config.class.getName());

    private String getSSmParameter(String key) {
        GetParameterResult parameterResult = AWSSimpleSystemsManagementClientBuilder.defaultClient().getParameter(new GetParameterRequest()
                .withName(key));
        return parameterResult.getParameter().getValue();
    }

    private Config() {
        String[] primaryParams = getSSmParameter("BrokerSyncPrimaryBrokerParams").split(";");
        primaryBrokerURL = primaryParams[0];
        primaryBrokerUsername = primaryParams[1];
        primaryBrokerPassword = primaryParams[2];
        String[] standbyParams = getSSmParameter("BrokerSyncStandbyBrokerParams").split(";");
        standbyBrokerURL = standbyParams[0];
        standbyBrokerUsername = standbyParams[1];
        standbyBrokerPassword = standbyParams[2];
        String[] cacheParams = getSSmParameter("BrokerSyncCacheParams").split(",");
        cacheConfigURL = cacheParams[0];
        cacheConfigPort = cacheParams[1];

        String tempDrQueues = getSSmParameter("BrokerSyncDRQueues");
        if (tempDrQueues.contains(",")) {
            String[] drQueues = tempDrQueues.split(",");
            for(int i=0; i < drQueues.length; i++) {
                this.drQueues.add(drQueues[i]);
            }
        } else {
            this.drQueues.add(tempDrQueues);
        }

        String[] miscParams = getSSmParameter("BrokerSyncMiscParams").split(",");
        threadsPerQueue = Integer.valueOf(miscParams[0]).intValue();
        retryTimeout = Integer.valueOf(miscParams[1]).intValue();
        cwTag = miscParams[2];
    }

    public String getCWTag() {
        return cwTag;
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

    public String getCacheConfigURL() {
        return cacheConfigURL;
    }

    public Integer getCacheConfigPort() {
        return Integer.valueOf(cacheConfigPort);
    }

    public void dump() {
        logger.info(this::getPrimaryBrokerURL);
        logger.info(this::getStandbyBrokerURL);
        logger.info(this::getCacheConfigURL);
    }
}
