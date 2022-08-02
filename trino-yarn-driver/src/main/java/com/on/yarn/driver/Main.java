package com.on.yarn.driver;

public class Main {
    public static void main(String[] args) throws Exception {
        String hadoopConfDir = null;
        if (args.length != 1) {
            System.out.println("Usage: yarn jar trino-yarn-driver.jar com.on.yarn.driver.Main <hadoopConfDir>");
            System.exit(1);
        }
        TrinoSession.getDriverInfo(hadoopConfDir);
    }
}
