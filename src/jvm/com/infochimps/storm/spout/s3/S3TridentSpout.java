package com.infochimps.storm.spout.s3;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

public class S3TridentSpout implements ITridentSpout<Map>{

    Map<String, String> credentials;
    
    public S3TridentSpout(Map<String, String> credentials) {
        this.credentials = credentials;
    }

    @Override
    public storm.trident.spout.ITridentSpout.BatchCoordinator<Map> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new S3BatchCoordintor(txStateId, conf, context, credentials);
    }

    @Override
    public storm.trident.spout.ITridentSpout.Emitter<Map> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new S3BatchEmitter(txStateId, conf, context, credentials);
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return null;
    }

}
