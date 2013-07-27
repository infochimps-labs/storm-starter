package com.infochimps.storm.spout.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;

import backtype.storm.tuple.Values;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Reader {

    public static void main(String[] args) throws Exception {

        final String TEST_ACCESS_KEY = "AKIAIAVKQWKGWHTUSPWA"; // infochimps:s3testuser
        final String TEST_SECRET_KEY = "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"; // infochimps:s3testuser
        final String TEST_BUCKET_NAME = "s3spout.test.chimpy.us";
        AmazonS3Client client = new AmazonS3Client(new BasicAWSCredentials(TEST_ACCESS_KEY, TEST_SECRET_KEY));
        String marker = "";

        while(true){
            System.out.println("------------------------------------");
            System.out.println("Getting data from marker : " + marker);
            List<String> ids = getMetaData(TEST_BUCKET_NAME, client, marker);
            if(ids.size() != 0){
                
                marker = ids.get(ids.size() - 1);
            }
            
            Thread.sleep(30000);
        }
    }

    private static List<String> getMetaData(final String TEST_BUCKET_NAME, AmazonS3Client client, String marker) {
        ArrayList<String> ids = new ArrayList<String>();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(TEST_BUCKET_NAME).withMaxKeys(2)
                .withMarker(marker)
                .withPrefix("/2013");
        ObjectListing objectListing;
        do {
            objectListing = client.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
                ids.add(objectSummary.getKey());
            }
            listObjectsRequest.setMarker(objectListing.getNextMarker());
            // System.out.println(listObjectsRequest.getMarker());
        } while (objectListing.isTruncated());

        return ids;
    }

}