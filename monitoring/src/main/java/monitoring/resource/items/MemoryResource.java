package monitoring.resource.items;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;

public class MemoryResource implements ResourceUsage{
    private double memoryUsage;

    @Override
    public double getResource() {
        OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        double totalMemorySize = osBean.getTotalPhysicalMemorySize();
        double freeMemorySize = osBean.getFreePhysicalMemorySize();
        double usedMemorySize = totalMemorySize - freeMemorySize;

        this.memoryUsage =  (usedMemorySize / totalMemorySize) * 100;
        return this.memoryUsage;
    }

}
