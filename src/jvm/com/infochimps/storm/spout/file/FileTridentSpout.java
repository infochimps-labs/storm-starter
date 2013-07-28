package com.infochimps.storm.spout.file;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

public class FileTridentSpout implements ITridentSpout<String>{

    private String _dir ;
    
    public FileTridentSpout(String _dir) {
        super();
        this._dir = _dir;
    }
    
    @Override
    public storm.trident.spout.ITridentSpout.BatchCoordinator<String> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        System.out.println("Spout.getCoordinator() called - txStateId : " + txStateId);
        return new FileBatchCoordinator(_dir);
    }

    @Override
    public storm.trident.spout.ITridentSpout.Emitter<String> getEmitter(String txStateId, Map conf, TopologyContext context) {
        System.out.println("Spout.getEmitter() called - txStateId : " + txStateId);
        return new FileBatchEmitter(txStateId, conf, context);
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("line");
    }

}
