package com.infochimps.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import com.infochimps.storm.trident.spout.IBlobStore;
import com.infochimps.storm.trident.spout.IRecordizer;
import com.infochimps.storm.trident.spout.OpaqueTransactionalBlobSpout;
import com.infochimps.storm.trident.spout.S3BlobStore;
import com.infochimps.storm.trident.spout.StartPolicy;
import com.infochimps.storm.trident.spout.WukongRecordizer;

public class S3TestTopology {
    public static class Split extends BaseFunction {
        private static final Logger LOG = LoggerFactory.getLogger(S3TestTopology.class);

        int line=0;
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);

           System.out.println("Split execute called yo: " + sentence); 
//            LOG.info("Split execute called yo: " + sentence + tuple); 
            line +=1;
//            System.out.print(".");
//            if(line % 150 == 0)System.out.println("");
             try {
                 Thread.sleep(100);
             } catch (InterruptedException e) {
                 e.printStackTrace();
             }
            for (String word : sentence.split(" ")) { 
                collector.emit(new Values(word));
            }
        }
    }

    public static void main(String[] args) throws Exception, InvalidTopologyException {
        final String TEST_ACCESS_KEY = ExampleConfig.getString("aws.access.key"); // infochimps:s3testuser 
        final String TEST_SECRET_KEY = ExampleConfig.getString("aws.secret.key"); // infochimps:s3testuser 
        final String TEST_BUCKET_NAME = ExampleConfig.getString("aws.bucket.name"); 
        final String TEST_ENDPOINT = ExampleConfig.getString("aws.endpoint.name"); 
        String prefix = ExampleConfig.getString("aws.prefix"); 

        
        System.out.println(TEST_ACCESS_KEY + "--" + TEST_SECRET_KEY );
        IRecordizer rc = new WukongRecordizer();

        IBlobStore bs = new S3BlobStore(prefix,TEST_BUCKET_NAME,TEST_ENDPOINT, TEST_ACCESS_KEY, TEST_SECRET_KEY);
//        OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs, StartPolicy.RESUME, null);
        OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs, StartPolicy.EARLIEST, null);
//        OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs);
//        OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs, StartPolicy.EXPLICIT, "/x/y_meta/1377207157853/b.txt.meta");

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology
            .newStream("s3spout1", spout) 
            .parallelismHint(1)
            .each(new Fields("line"), new Split(), new Fields("word"))  //$NON-NLS-2$
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
