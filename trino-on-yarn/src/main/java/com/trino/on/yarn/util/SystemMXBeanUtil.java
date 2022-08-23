package com.trino.on.yarn.util;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

public class SystemMXBeanUtil {

    public static int getFreePhysicalMemorySize() {
        OperatingSystemMXBean osmb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long freePhysicalMemorySize = osmb.getFreePhysicalMemorySize();
        return (int) (freePhysicalMemorySize / (1024 * 1024 * 1024));
    }
}
