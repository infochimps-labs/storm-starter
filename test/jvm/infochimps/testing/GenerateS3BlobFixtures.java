package infochimps.testing;

import java.io.File;
import java.io.IOException;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class GenerateS3BlobFixtures {

    public static void main(String[] args) throws IOException {

        String TEST_ACCESS_KEY = "AKIAIAVKQWKGWHTUSPWA"; // infochimps:s3testuser
        String TEST_SECRET_KEY = "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"; // infochimps:s3testuser
        String TEST_BUCKET_NAME = "s3spout.test.chimpy.us";
        String _dir = "data";
        String _prefix = "x/test";

        AmazonS3Client client = new AmazonS3Client(new BasicAWSCredentials(TEST_ACCESS_KEY, TEST_SECRET_KEY));

        File folder = new File(_dir);        
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile()) {
                String uploadFileName = file.getName();
                System.out
                        .println("Uploading a new object to S3 from a file : "
                                + uploadFileName);
                long l = System.currentTimeMillis();

                client.putObject(new PutObjectRequest(TEST_BUCKET_NAME, _prefix
                        + l + "/" + uploadFileName, file));
                client.putObject(new PutObjectRequest(TEST_BUCKET_NAME, _prefix +"_meta/"
                        + l + "/" + uploadFileName + ".meta", file));
            }
        }

    }
}