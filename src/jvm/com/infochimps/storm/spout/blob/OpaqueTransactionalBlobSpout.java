package com.infochimps.storm.spout.blob;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
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

@SuppressWarnings({ "unchecked", "rawtypes" })
public class OpaqueTransactionalBlobSpout
        implements
        IOpaquePartitionedTridentSpout<Map, OpaqueTransactionalBlobSpout.SinglePartition, Map> {
    
    private static final Logger LOG = LoggerFactory.getLogger(OpaqueTransactionalBlobSpout.class);

    BlobStore _bs;

    public OpaqueTransactionalBlobSpout(BlobStore blobStore ) {
        _bs = blobStore;
    }

    @Override
    public IOpaquePartitionedTridentSpout.Emitter<Map, SinglePartition, Map> getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context, _bs);
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
     
        private String _compId;

        private BlobStore _blobStore;

        private static final String CHARACTER_SET = "UTF-8";

        public Emitter(Map conf, TopologyContext context, BlobStore blobStore) {
             
            _compId = context.getThisComponentId();
            _blobStore = blobStore;
            _blobStore.initialize();
        }

        @Override
        public Map emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, SinglePartition partition, Map lastPartitionMeta) {
            
            String txId = ""+tx.getTransactionId();
            
            /** Get metadata file. **/
            boolean currentBatchFailed = false;
            
            String marker = null;
            boolean lastBatchFailed = false;
                    
            if(lastPartitionMeta != null){
                marker = (String) lastPartitionMeta.get("marker");
                lastBatchFailed = (Boolean) lastPartitionMeta.get("lastBatchFailed");
            }
            
            boolean isDataAvailable = true;
            BufferedReader reader = null;
            try{
                LOG.debug(Utils.logString("emitPartitionBatch", _compId, txId, "prev", marker));
                // Update the marker if the last batch succeeded, otherwise retry.
                if (!lastBatchFailed) {

                    String tmp = _blobStore.getNextBlobMarker(marker);

                    // marker stays same if no new files are available.
                    marker =  (tmp == null) ? marker : tmp;
                    isDataAvailable = (tmp == null) ? false : true;
                }
                
                LOG.debug(Utils.logString("emitPartitionBatch", _compId, txId ,"new", marker));
                
                /** Download the actual file **/
                if(isDataAvailable){
                    Map<String, Object> context = new HashMap<String,Object>();
                    context.put("txId", txId);
                    context.put("compId", _compId);
                    
                    InputStream blobDataStream = _blobStore.getBlob(marker, context);
                    
                    
                    reader = new BufferedReader(new InputStreamReader(blobDataStream, CHARACTER_SET));
                    
                    String line;
                    while ((line = reader.readLine()) != null) {
                        collector.emit(new Values(line));
                        LOG.trace(Utils.logString("emitPartitionBatch", _compId, txId,"emitted", line));
                    }
                }
                
            } catch (Throwable t) {
                //Catch everything that can go wrong.
                LOG.error(Utils.logString("emitPartitionBatch", _compId, txId,"Error in reading file from S3."), t);
                currentBatchFailed = true;
                
            } finally {
                if (reader != null)
                    try { reader.close();} catch (IOException e) {
                        LOG.error(Utils.logString("emitPartitionBatch", _compId, txId,"Error in closing the data stream."), e);
                    }
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
