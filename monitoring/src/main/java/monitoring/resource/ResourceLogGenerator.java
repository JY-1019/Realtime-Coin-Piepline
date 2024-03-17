package monitoring.resource;

import monitoring.DataGenerator;
import monitoring.resource.items.ResourceUsage;
import monitoring.resource.items.CpuResource;
import monitoring.resource.items.MemoryResource;
import monitoring.resource.items.DiskResource;

import org.json.JSONObject;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class ResourceLogGenerator implements DataGenerator {

    private JSONObject resourceLog;
    private double cpu;
    private double memory;
    private double disk;

    @Override
    public void generateData() {
        ResourceUsage cpuResource = new CpuResource();
        ResourceUsage memoryResource = new MemoryResource();
        ResourceUsage diskResource = new DiskResource();

        this.disk = diskResource.getResource();

        int numSamples = 10; // 샘플링 횟수
        double totalMemoryUsage = 0;

//        long startTime = System.currentTimeMillis();
//        double startCpuUsage = cpuResource.getResource();
        for (int i = 0; i < numSamples; i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            totalMemoryUsage+=memoryResource.getResource();

        }
//        long endTime = System.currentTimeMillis();
//        double endCpuUsage = cpuResource.getResource();
//        double elapsedTimeInSeconds = (endTime - startTime) / 1000.0;
        this.cpu = cpuResource.getResource() ;
        this.memory = totalMemoryUsage/numSamples;

    }

    @Override
    public String getData() {
        generateData();
        this.resourceLog = new JSONObject();
        this.resourceLog.put("cpu", cpu);
        this.resourceLog.put("memory", memory);
        this.resourceLog.put("disk", disk);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        this.resourceLog.put("timestamp", Instant.now().atZone(ZoneId.systemDefault()).format(formatter));
        return this.resourceLog.toString();
    }
}
