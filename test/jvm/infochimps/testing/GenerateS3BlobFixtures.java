package infochimps.testing;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.infochimps.examples.ExampleConfig;

public class GenerateS3BlobFixtures {

    public static void main(String[] args) throws IOException {

        final String TEST_ACCESS_KEY = ExampleConfig.getString("aws.access.key"); // infochimps:s3testuser 
        final String TEST_SECRET_KEY = ExampleConfig.getString("aws.secret.key"); // infochimps:s3testuser 
        final String TEST_BUCKET_NAME = ExampleConfig.getString("aws.bucket.name"); 
        final String TEST_ENDPOINT = ExampleConfig.getString("aws.endpoint.name"); 

        String _prefix = ExampleConfig.getString("aws.prefix"); 
        String _dir = "data";

        AmazonS3Client client = new AmazonS3Client(new BasicAWSCredentials(TEST_ACCESS_KEY, TEST_SECRET_KEY));
        client.setEndpoint(TEST_ENDPOINT);
        File folder = new File(_dir); 
        Collection<File> listOfFiles = FileUtils.listFiles(folder, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

        for (File file : listOfFiles) {
            if (file.isFile()) {
                String uploadFileName = file.getName();
                System.out
                        .println("Uploading a new object to S3 from a file : "
                                + uploadFileName);
                long l = System.currentTimeMillis();

                client.putObject(new PutObjectRequest(TEST_BUCKET_NAME, _prefix +"/"
                        + l + "/" + uploadFileName, file));
                client.putObject(new PutObjectRequest(TEST_BUCKET_NAME, _prefix +"_meta/"
                        + l + "/" + uploadFileName + ".meta", file));
            }
        }

    }
}