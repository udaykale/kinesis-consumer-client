package com.udaykale.kinesisconsumerclient;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.udaykale.kinesisconsumerclient.master.MasterClientManager;
import com.udaykale.kinesisconsumerclient.slave.SlaveClientManager;
import com.udaykale.kinesisconsumerclient.slave.SlaveStateReadResponse;
import com.udaykale.kinesisconsumerclient.slave.SlaveStateWriteRequest;
import com.udaykale.kinesisconsumerclient.slave.SlaveStateWriteResponse;

import java.util.List;

public final class KinesisConsumerClient implements AutoCloseable {

    private final KinesisConsumerClientConf kinesisConsumerClientConf;
    private final MasterClientManager masterClientManager;
    private final SlaveClientManager slaveClientManager;
    private final AmazonKinesis amazonKinesisClient;

    private boolean isInitialized;

    KinesisConsumerClient(MasterClientManager masterClientManager, SlaveClientManager slaveClientManager,
                          KinesisConsumerClientConf kinesisConsumerClientConf, AmazonKinesis amazonKinesisClient) {
        this.kinesisConsumerClientConf = kinesisConsumerClientConf;
        this.masterClientManager = masterClientManager;
        this.slaveClientManager = slaveClientManager;
        this.amazonKinesisClient = amazonKinesisClient;
        this.isInitialized = false;
    }

    public static KinesisConsumerClient of(KinesisConsumerClientConf kinesisConsumerClientConf,
                                           AmazonKinesis amazonKinesisClient) {
        // TODO: Read the conf to init the master and slave managers
        // TODO: Validations
        MasterClientManager masterClientManager = null;
        SlaveClientManager slaveClientManager = null;

        return new KinesisConsumerClient(masterClientManager, slaveClientManager,
                kinesisConsumerClientConf, amazonKinesisClient);
    }

    private void init() {
        if (isInitialized) {
            boolean isMaster = slaveClientManager.register();
            if (isMaster) {
                // TODO: Run the MasterConfManager in a different thread
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                    }
                };
            }
            isInitialized = true;
            // TODO: Remove sys out
            System.out.println("Registered for:" + slaveClientManager.getClientId() + slaveClientManager.getGroupId());
        }
    }

    private synchronized List<Record> read() {
        // TODO: validations
        int maxSize = kinesisConsumerClientConf.maxRecordCount();
        int maxDuration = kinesisConsumerClientConf.maxReadDuration();

        init();

//        TODO: Move some of this code to the client manager
//
//        String streamName = kinesisConsumerClientConf.streamName();
//        Shard shard = kinesisConsumerClientConf.shard();
//        String shardIteratorType = kinesisConsumerClientConf.shardIteratorType();
//
//        String shardIterator;
//        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
//        getShardIteratorRequest.setStreamName(streamName);
//        getShardIteratorRequest.setShardId(shard.getShardId());
//        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
//
//        GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
//        shardIterator = getShardIteratorResult.getShardIterator();

        SlaveStateReadResponse slaveStateReadResponse = slaveClientManager.readState();

        // TODO: read data from the allocated shards
        // TODO: read until the max size or max duration is satisfied
        // TODO: throw an exception if the shards are read but not state saved
        SlaveStateWriteRequest slaveStateWriteRequest = new SlaveStateWriteRequest();
        SlaveStateWriteResponse slaveStateWriteResponse = slaveClientManager.writeState(slaveStateWriteRequest);

        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(slaveStateReadResponse.getShardIterator());

        GetRecordsResult getRecordsResult = amazonKinesisClient.getRecords(getRecordsRequest);
        return getRecordsResult.getRecords();
    }

    @Override
    public void close() throws Exception {
        slaveClientManager.unregister();
        masterClientManager.close();
    }
}
