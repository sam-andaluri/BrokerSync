package com.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class MessageSyncStream {

    private  String streamArn;
    private  Regions awsRegion;
    private  AWSCredentialsProvider awsCredentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

    public MessageSyncStream(String streamArn, Regions regions) {
        this.streamArn = streamArn;
        awsRegion = regions;
    }

    void start()  {

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(awsRegion)
                .build();
        AmazonCloudWatch cloudWatchClient = AmazonCloudWatchClientBuilder.standard()
                .withRegion(awsRegion)
                .build();
        AmazonDynamoDBStreams dynamoDBStreamsClient = AmazonDynamoDBStreamsClientBuilder.standard()
                .withRegion(awsRegion)
                .build();
        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreamsClient);

        IRecordProcessorFactory recordProcessorFactory = new StreamsRecordProcessorFactory(dynamoDBClient);

        KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration("message-sync-stream",
                streamArn,
                awsCredentialsProvider,
                "messsage-sync-worker")
                .withMaxRecords(1000)
                .withIdleTimeBetweenReadsInMillis(500)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        Worker worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(recordProcessorFactory, workerConfig, adapterClient, dynamoDBClient, cloudWatchClient);
        Thread t = new Thread(worker);
        t.start();
    }
}


