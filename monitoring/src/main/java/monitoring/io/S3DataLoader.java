package monitoring.io;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class S3DataLoader {
    private String bucketName;
    private String folderName;
    private AmazonS3 s3Client;

    public S3DataLoader() {
        EnvManager envManager = new EnvManager();
        String accessKey = envManager.getValueByKey("accessKey");
        String secretKey = envManager.getValueByKey("secretKey");
        Region region = Region.getRegion(Regions.AP_NORTHEAST_2);  // Region 설정

        this.bucketName = envManager.getValueByKey("bucket");
        this.folderName = envManager.getValueByKey("folder");
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

        // AmazonS3Client 생성
        this.s3Client = new AmazonS3Client(awsCredentials);
        this.s3Client.setRegion(region);

    }

    public void upload(String content) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        String objectKey = this.folderName + Instant.now().atZone(ZoneId.systemDefault()).format(formatter) + ".json";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes());

        // ObjectMetadata 생성 및 content length 설정
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length());

        // PutObjectRequest 생성
        PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, inputStream, metadata);

        try {
            s3Client.putObject(request);
            System.out.println("Object uploaded to S3: " + objectKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}