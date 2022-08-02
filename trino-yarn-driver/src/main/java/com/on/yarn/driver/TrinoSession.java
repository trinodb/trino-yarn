package com.on.yarn.driver;

public class TrinoSession {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: yarn jar trino-yarn-driver.jar com.on.yarn.driver.Main <hadoopConfDir>");
            System.exit(1);
        }
        String apiInfo = null;
        if (args[0].equalsIgnoreCase("api")) {
            apiInfo = TrinoSessionApi.getApiInfo(args[1]);
        } else {
            apiInfo = TrinoSessionDriver.getDriverInfo(args[1]);
        }
        System.out.println(apiInfo);
    }
}
