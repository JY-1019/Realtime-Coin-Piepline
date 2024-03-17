package monitoring.resource.items;

import java.io.File;

public class DiskResource implements ResourceUsage{
    private double diskUsage;

    public DiskResource() {
        File root = new File("/");
        long totalSpace = root.getTotalSpace();
        long freeSpace = root.getFreeSpace();

        // 단위: MB
        double diskTotal = (double) totalSpace / (1024 * 1024);
        double diskFree = (double) freeSpace / (1024 * 1024);

        this.diskUsage = ((diskTotal - diskFree) / diskTotal) * 100;
    }
    @Override
    public double getResource() {
        return this.diskUsage;
    }
}

