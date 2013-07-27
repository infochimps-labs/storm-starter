package com.infochimps.storm.spout.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

public class S3BatchEmitter implements Emitter<Map> {


    private AmazonS3Client _client;

    public S3BatchEmitter(String txStateId, Map conf, TopologyContext context, Map<String, String> credentials) {
        _client = new AmazonS3Client(new BasicAWSCredentials(credentials.get("accessKey"), credentials.get("secretKey")));
    }

    @Override
    public void emitBatch(TransactionAttempt tx, Map coordinatorMeta, TridentCollector collector) {
        /*
         * Download an object - When you download an object, you get all of
         * the object's metadata and a stream from which to read the contents.
         * It's important to read the contents of the stream as quickly as
         * possibly since the data is streamed directly from Amazon S3 and your
         * network connection will remain open until you read all the data or
         * close the input stream.
         *
         * GetObjectRequest also supports several other options, including
         * conditional downloading of objects based on modification times,
         * ETags, and selectively downloading a range of an object.
         */
        System.out.println("Downloading an object" + coordinatorMeta);
        
        String bucketName = (String) coordinatorMeta.get("bucketName");
        String key = (String) coordinatorMeta.get("key");
        
        S3Object object = _client.getObject(new GetObjectRequest(bucketName, key));
        
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        while (true) {
            String line;
            try {
                line = reader.readLine();
                if (line == null)
                    break;
                collector.emit(new Values(line));
                System.out.println("Emitted    " + line);
            } catch (IOException e) {
                e.printStackTrace();
            }
            
        }
    }

    @Override
    public void success(TransactionAttempt tx) {

    }

    @Override
    public void close() {

    }

}
