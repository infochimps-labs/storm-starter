package com.infochimps.storm.spout.s3;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3Writer{
    private static String uploadFileName ="/Users/sa/code/storm-starter/queue.log.";
    
    public static void main(String[] args) throws IOException {
        final String TEST_ACCESS_KEY = "AKIAIAVKQWKGWHTUSPWA"; // infochimps:s3testuser
        final String TEST_SECRET_KEY = "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"; // infochimps:s3testuser
        final String TEST_BUCKET_NAME = "s3spout.test.chimpy.us";
        AmazonS3Client client = new AmazonS3Client(new BasicAWSCredentials(TEST_ACCESS_KEY, TEST_SECRET_KEY));
            for (int j = 1; j <= 9; j++) {
                System.out.println("Uploading a new object to S3 from a file : " + uploadFileName+j);
                
            File file = new File(uploadFileName+j);
            
            client.putObject(new PutObjectRequest(
                    TEST_BUCKET_NAME, "/2013/x/"+new Timestamp(new Date().getTime())+uploadFileName+j, file));
            }

        }
    }