package com.trino.on.yarn.util;

import oshi.SystemInfo;

public class OSUtil {

    public static int getAvailableMemorySize() {
        SystemInfo systemInfo = new SystemInfo();
        long available = systemInfo.getHardware().getMemory().getAvailable();
        return (int) (available / (1024 * 1024));
    }
}
