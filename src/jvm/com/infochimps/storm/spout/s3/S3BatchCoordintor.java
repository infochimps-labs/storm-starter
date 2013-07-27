package com.infochimps.storm.spout.s3;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import storm.trident.spout.ITridentSpout.BatchCoordinator;

public class S3BatchCoordintor implements BatchCoordinator<Map> {

    public S3BatchCoordintor(String txStateId, Map conf, TopologyContext context, Map<String, String> credentials) {
    }
    

    @Override
    public Map initializeTransaction(long txid, Map prevMetadata, Map currMetadata) {
        return null;
    }

    @Override
    public void success(long txid) {

    }

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {

    }

}
