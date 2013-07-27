package com.infochimps.storm.spout.file;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

public class FileBatchEmitter implements Emitter<String> {

    public FileBatchEmitter(String txStateId, Map conf, TopologyContext context) {
        System.out
        .printf("Emitter [%s] created with txStateId : %s\n", this
                .hashCode(), txStateId);
    }

    @Override
    public void emitBatch(TransactionAttempt tx, String coordinatorMeta, TridentCollector collector) {
        System.out
        .printf("BlobEmitter [%s].emitBatch() called with tx :%s meta : %s \n", this
                .hashCode(), tx, coordinatorMeta);
        
        if (coordinatorMeta == null) {
            return;
        }
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(coordinatorMeta));
            String line;
            while ((line = br.readLine()) != null) {
                collector.emit(new Values(line));
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void success(TransactionAttempt tx) {

    }

    @Override
    public void close() {

    }

}
