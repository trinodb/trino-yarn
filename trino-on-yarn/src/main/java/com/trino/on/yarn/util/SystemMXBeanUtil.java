package com.trino.on.yarn.util;

import oshi.SystemInfo;

public class SystemMXBeanUtil {

    public static int getAvailablePhysicalMemorySize() {
        SystemInfo systemInfo = new SystemInfo();
        long available = systemInfo.getHardware().getMemory().getAvailable();
        return (int) (available / (1024 * 1024));
    }
}
