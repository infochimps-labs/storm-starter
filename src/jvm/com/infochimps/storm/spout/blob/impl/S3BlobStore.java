package com.infochimps.storm.spout.blob.impl;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.infochimps.storm.spout.blob.BlobStore;

public class S3BlobStore implements BlobStore{
    
    private static final Logger LOG = LoggerFactory.getLogger(S3BlobStore.class);


    private String _prefix;
    private String _bucket;
    private AmazonS3Client _client;


    private String _secretKey;


    private String _accessKey;
    
    public S3BlobStore(String prefix, String bucket, String accessKey, String secretKey) {
        _prefix = prefix;
        _bucket = bucket;
        _accessKey = accessKey;
        _secretKey = secretKey;

    }

    @Override
    public String getNextBlobMarker(String currentMarker) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(_bucket).withMaxKeys(1)
                .withMarker(currentMarker).withPrefix(_prefix + "_meta");

        ObjectListing listing = _client.listObjects(listObjectsRequest);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        if (summaries.size() != 0) {
            S3ObjectSummary summary = summaries.get(0);
            return summary.getKey();
        }

        return null;
    }

    @Override
    public InputStream getBlob(String blobMarker, Map<String, Object> context) {
        //Figure out the actual file name. (Make sure you only remove first _meta and last .meta)

        String dataKey = blobMarker.substring(0, blobMarker.lastIndexOf(".meta")).replaceAll(_prefix + "_meta", _prefix);

        LOG.info(Utils.logString("getBlob", context.get("compId").toString(), context.get("txId").toString(),"Reading S3 file", dataKey));
        
        // Read it and send to the topology line by line.
        S3Object object = _client.getObject(new GetObjectRequest(_bucket, dataKey));
        
        return object.getObjectContent();
    }

    @Override
    public boolean initialize() {
        _client = new AmazonS3Client(new BasicAWSCredentials(_accessKey, _secretKey));

        // Blow up if the AmazonS3Client is not configured properly.
        _client.getBucketAcl(_bucket);
        return false;
    }

}
