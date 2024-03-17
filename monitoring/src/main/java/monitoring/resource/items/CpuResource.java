package monitoring.resource.items;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;

public class CpuResource implements ResourceUsage {
    private double cpuUsage;

    @Override
    public double getResource() {
        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            this.cpuUsage = osBean.getSystemLoadAverage() * 100;
        return this.cpuUsage;
    }
}