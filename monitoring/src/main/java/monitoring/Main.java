package monitoring;
import monitoring.io.S3DataLoader;
import monitoring.resource.ResourceLogGenerator;
public class Main {
    public static void main(String[] args) {
        while (true) {
            ResourceLogGenerator r = new ResourceLogGenerator();
            System.out.println(r.getData());
            S3DataLoader s3DataLoader = new S3DataLoader();
            s3DataLoader.upload(r.getData());
        }
    }
}