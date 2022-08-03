package com.on.yarn.driver;

import cn.hutool.core.util.ArrayUtil;

/**
 * sdk方式: /usr/bin/yarn jar /mnt/dss/trino-yarn-driver-1.0.0.jar com.on.yarn.driver.TrinoSession  api  /etc/hadoop/conf  10.121.100.83:8088
 * 接口方式: /usr/bin/yarn jar /mnt/dss/trino-yarn-driver-1.0.0.jar com.on.yarn.driver.TrinoSession  driver  /etc/hadoop/conf
 */
public class TrinoSession {
    public static void main(String[] args) throws Exception {
        if (!(args.length == 2 || args.length == 3)) {
            System.out.println("Usage: yarn jar trino-yarn-driver.jar com.on.yarn.driver.Main (args.length == 2 || args.length == 3)" + ArrayUtil.toString(args));
            System.exit(1);
        }
        String apiInfo = null;
        if (args[0].equalsIgnoreCase("api")) {
            apiInfo = TrinoSessionApi.getApiInfo(args[1], args[2]);
        } else {
            apiInfo = TrinoSessionDriver.getDriverInfo(args[1]);
        }
        System.out.println(apiInfo);
    }
}
