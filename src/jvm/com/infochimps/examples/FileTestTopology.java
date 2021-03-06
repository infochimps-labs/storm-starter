package com.infochimps.examples;

import storm.starter.trident.InstrumentedMemoryMapState;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.infochimps.storm.trident.spout.FileBlobStore;
import com.infochimps.storm.trident.spout.IBlobStore;
import com.infochimps.storm.trident.spout.OpaqueTransactionalBlobSpout;

public class FileTestTopology {
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);

            System.out.println("Split execute called on : " + sentence + tuple);
            // try {
            //     Thread.sleep(3);
            // } catch (InterruptedException e) {
            //     e.printStackTrace();
            // }
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static void main(String[] args) throws Exception, InvalidTopologyException {
        IBlobStore bs = new FileBlobStore("/Users/sa/code/storm-starter/data");
        OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology
            .newStream("spout1", spout)
            .parallelismHint(1)
            .each(new Fields("line"), new Split(), new Fields("word"))
            .groupBy(new Fields("word"))
            .persistentAggregate(new InstrumentedMemoryMapState.Factory(), new Count(), new Fields("count"))
            .parallelismHint(1);

        Config conf = new Config();
        conf.setMessageTimeoutSecs(10);
        // conf.setMaxSpoutPending(3);
        System.out.println("Topology created");
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, topology.build());
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, topology.build());
        }
    }
}
