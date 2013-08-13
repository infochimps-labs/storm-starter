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
import com.infochimps.storm.trident.spout.IRecordizer;
import com.infochimps.storm.trident.spout.OpaqueTransactionalBlobSpout;
import com.infochimps.storm.trident.spout.S3BlobStore;
import com.infochimps.storm.trident.spout.WukongRecordizer;

public class WukongRecordizerTestTopology {
    public static class CombineMetaData extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String content = tuple.getStringByField("content");
            String metadata = tuple.getStringByField("metadata");
            Integer lineNumber = tuple.getIntegerByField("linenumber");

            System.out.println(String.format("CombineMetaData called - %s\t%s\t%s\n", metadata, content, lineNumber));
            // try {
            //     Thread.sleep(3);
            // } catch (InterruptedException e) {
            //     e.printStackTrace();
            // }
            collector.emit(new Values(String.format("%s\t%s", metadata, content)));
        }
    }
    
    public static class SeparateMetaData extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String line = tuple.getString(0);
            String[] tmp = line.split("\t");
            System.out.println(String.format("SeparateMetaData called - content : %s --- metadata: %s\n ", tmp[1], tmp[0]));
            // try {
            //     Thread.sleep(3);
            // } catch (InterruptedException e) {
            //     e.printStackTrace();
            // }
            collector.emit(new Values(tmp[1]));
        }
    }
    
    

    public static void main(String[] args) throws Exception, InvalidTopologyException {
        
        
//        final String TEST_ACCESS_KEY = ExampleConfig.getString("aws.access.key"); // infochimps:s3testuser 
//        final String TEST_SECRET_KEY = ExampleConfig.getString("aws.secret.key"); // infochimps:s3testuser 
//        final String TEST_BUCKET_NAME = ExampleConfig.getString("aws.bucket.name"); 
//        String prefix = ExampleConfig.getString("aws.prefix"); 
//
//        IBlobStore bs = new S3BlobStore(prefix,TEST_BUCKET_NAME, TEST_ACCESS_KEY, TEST_SECRET_KEY);

        
        IBlobStore bs = new FileBlobStore("/Users/sa/code/storm-starter/data/small");
        IRecordizer rc = new WukongRecordizer();
        OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs, rc);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology
            .newStream("spout1", spout)
            .parallelismHint(1)
            .each(rc.getFields(), new CombineMetaData(), new Fields("metadataword"))
            .each(new Fields("metadataword"), new SeparateMetaData(), new Fields("word"))
            //.groupBy(new Fields("word"))
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
