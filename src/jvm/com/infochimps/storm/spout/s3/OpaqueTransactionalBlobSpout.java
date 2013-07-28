package com.infochimps.storm.spout.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class OpaqueTransactionalBlobSpout
        implements
        IOpaquePartitionedTridentSpout<Map, OpaqueTransactionalBlobSpout.SinglePartition, Map> {
    
    private static final Logger LOG = LoggerFactory.getLogger(OpaqueTransactionalBlobSpout.class);

    private String _prefix;
    private String _bucket;
    private String _accessKey;
    private String _secretKey;

    public OpaqueTransactionalBlobSpout(String _accessKey, String _secretKey, String _bucket, String _prefix ) {
        this._prefix = _prefix;
        this._bucket = _bucket;
        this._accessKey = _accessKey;
        this._secretKey = _secretKey;
    }

    @Override
    public IOpaquePartitionedTridentSpout.Emitter<Map, SinglePartition, Map> getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context, _accessKey, _secretKey, _bucket, _prefix);
    }

    @Override
    public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator();
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("line");
    }

    public class Emitter implements
            IOpaquePartitionedTridentSpout.Emitter<Map, SinglePartition, Map> {
     
        private AmazonS3Client _client;
        private String _S3Bucket;
        private String _S3Prefix;

        private static final String CHARACTER_SET = "UTF-8";

        public Emitter(Map conf, TopologyContext context, String accessKey, String secretKey, String bucket, String prefix) {
             
            _client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
            this._S3Bucket = bucket;
            this._S3Prefix = prefix;

            // This is here to check if the AmazonS3Client is configured properly, if not blow up.
            _client.getBucketAcl(bucket);
            
            
        }

        @Override
        public Map emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, SinglePartition partition, Map lastPartitionMeta) {
            
            /** Get metadata file. **/
            boolean currentBatchFailed = false;
            
            String marker = null;
            boolean lastBatchFailed = false;
                    
            if(lastPartitionMeta != null){
                marker = (String) lastPartitionMeta.get("marker");
                
                lastBatchFailed = (Boolean) lastPartitionMeta.get("lastBatchFailed");
            }
            
            boolean isDataAvailable = true;
            try{
                LOG.debug(Utils.logString("emitPartitionBatch", "OpaqueTransactionalBlobSpout", "-", "Old Metadata file", marker));
                
                // Update the marker if the last batch succeeded, otherwise retry.
                if(!lastBatchFailed){
                    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(_S3Bucket)
                    .withMaxKeys(1)
                    .withMarker(marker)
                    .withPrefix(_S3Prefix + "_meta");
                    ObjectListing listing = _client.listObjects(listObjectsRequest);
                    List<S3ObjectSummary> summaries = listing.getObjectSummaries();
                    
                    // marker stays same if no new files are available.
                    if (summaries.size() != 0) {
                        
                        S3ObjectSummary summary = summaries.get(0);
                        marker = summary.getKey();
                        
                    } else {
                        isDataAvailable = false;
                    }
                } 
                LOG.debug(Utils.logString("emitPartitionBatch", "OpaqueTransactionalBlobSpout", "-","New Metadata file", marker));
                
                /** Download the actual file **/
                if(isDataAvailable){
                    
                    //Figure out the actual file name. (Make sure you only remove first _meta and last .meta)
                    
                    String dataKey = marker.substring(0, marker.lastIndexOf(".meta")).replaceAll(_S3Prefix + "_meta", _S3Prefix);
                    LOG.info(Utils.logString("emitPartitionBatch", "OpaqueTransactionalBlobSpout", "-","Reading S3 file", dataKey));
                    
                    // Read it and send to the topology line by line.
                    S3Object object = _client.getObject(new GetObjectRequest(_bucket, dataKey));
                    
                    BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent(), CHARACTER_SET));
                    while (true) {
                        String line;
                        line = reader.readLine();
                        if (line == null)
                            break;
                        collector.emit(new Values(line));
                        LOG.trace(Utils.logString("emitPartitionBatch", "OpaqueTransactionalBlobSpout", "-","Emitted", line));
                    }
                }
                
            } catch (Throwable t) {
                //Catch everything that can go wrong.
                LOG.error(Utils.logString("emitPartitionBatch", "OpaqueTransactionalBlobSpout", "-","Error in reading file from S3."), t);
                currentBatchFailed = true;
            }
            /** Update the lastMeta **/
            Map newPartitionMeta = new HashMap();
            newPartitionMeta.put("marker", marker);
            newPartitionMeta.put("lastBatchFailed", currentBatchFailed);
            
            return newPartitionMeta;
        }

        @Override
        public void refreshPartitions(List<SinglePartition> partitionResponsibilities) {
        }

        @Override
        public List<SinglePartition> getOrderedPartitions(Map allPartitionInfo) {
            
            // Need to provide at least one partition, otherwise it spins forever.
            ArrayList<SinglePartition> partition = new ArrayList<SinglePartition>();
            partition.add(new SinglePartition());
            return partition;
        }

        @Override
        public void close() {
        }

    }

    class Coordinator implements IOpaquePartitionedTridentSpout.Coordinator<Map> {

        @Override
        public boolean isReady(long txid) {
            return true;
        }

        @Override
        public Map getPartitionsForBatch() {
            return null;
        }

        @Override
        public void close() {
        }

    }
    
    class SinglePartition implements ISpoutPartition{

        @Override
        public String getId() {
            return "dummy";
        }

    }

}
